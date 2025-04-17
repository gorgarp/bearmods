import sys
import os
import tempfile
import shutil
import requests
import zipfile
import datetime
import traceback
from pathlib import Path
from PySide6.QtCore import Qt, Signal, QThread, Slot
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QProgressBar, QFileDialog, QListWidget, QSizePolicy, QListWidgetItem, QMessageBox, QTextEdit,
    QDialog
)
from PySide6.QtGui import QFont, QPalette, QColor, QIcon

import xxhash

def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

DEFAULT_PATH = r"C:\Program Files (x86)\Steam\steamapps\common\7 Days To Die\Mods"
CDN_URL = "https://bearmods.b-cdn.net/Client.zip"
BACKUP_PREFIX = "Mods_Backup_"

def user_desktop():
    return str(Path.home() / "Desktop")

def now_str():
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def copy_dir_all(src, dst, *, progress_callback=None):
    try:
        total_files = 0
        for root, dirs, files in os.walk(src):
            total_files += len(files)
        if not os.path.exists(dst):
            os.makedirs(dst)
        copied = 0
        for root, dirs, files in os.walk(src):
            rel = os.path.relpath(root, src)
            tgt_dir = os.path.join(dst, rel) if rel != '.' else dst
            if not os.path.exists(tgt_dir):
                os.makedirs(tgt_dir)
            for file in files:
                src_file = os.path.join(root, file)
                dst_file = os.path.join(tgt_dir, file)
                shutil.copy2(src_file, dst_file)
                copied += 1
                if progress_callback and total_files > 0:
                    percent = copied / total_files * 100
                    progress_callback(percent, f"Backing up... ({copied}/{total_files})")
        if progress_callback:
            progress_callback(100, f"Backup complete. ({copied}/{total_files})")
    except Exception as e:
        raise Exception(f"Failed to copy {src} to {dst}: {e}")

def create_zip_backup(src_dir, zip_path, *, progress_callback=None):
    try:
        total_files = 0
        for root, dirs, files in os.walk(src_dir):
            total_files += len(files)
        
        if progress_callback:
            progress_callback(0, f"Starting zip creation ({total_files} files)...")
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            file_count = 0
            for root, dirs, files in os.walk(src_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, src_dir)
                    zipf.write(file_path, rel_path)
                    file_count += 1
                    if progress_callback and total_files > 0:
                        percent = (file_count / total_files) * 100
                        progress_callback(percent, f"Creating backup zip: {file_count}/{total_files}")
        
        if progress_callback:
            progress_callback(100, f"Zip backup completed: {zip_path}")
        
        return zip_path
    except Exception as e:
        raise Exception(f"Failed to create zip backup: {e}")

def hash_file(path):
    try:
        h = xxhash.xxh3_64()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                h.update(chunk)
        return f"{h.intdigest():016x}"
    except Exception as e:
        return f"ERR({e})"

def scan_dir_with_hashes(root, filter_garbage=True):
    result = {}
    try:
        abs_root = Path(root).resolve()
        for base, dirs, files in os.walk(abs_root):
            rel_base = os.path.relpath(base, abs_root)
            skip_dir = False
            if filter_garbage:
                if rel_base == ".git" or rel_base.startswith(".git"):
                    skip_dir = True
            if skip_dir:
                continue
            for d in dirs:
                rel_path = os.path.normpath(os.path.join(rel_base, d)).replace("\\", "/")
                if filter_garbage:
                    if d in [".git", "__pycache__"] or d.lower().endswith(".tmp"):
                        continue
                result[rel_path] = {"is_dir": True, "hash": ""}
            for f in files:
                rel_path = os.path.normpath(os.path.join(rel_base, f)).replace("\\", "/")
                if filter_garbage:
                    if f in [".gitignore"] or f.lower().endswith(".tmp") or f.lower().endswith(".log"):
                        continue
                full_path = os.path.join(base, f)
                result[rel_path] = {"is_dir": False, "hash": hash_file(full_path)}
    except Exception as e:
        raise Exception(f"Failed to scan {root}: {e}")
    return result

class FileMeta:
    def __init__(self, rel_path, is_dir):
        self.rel_path = rel_path
        self.is_dir = is_dir

class WorkerThread(QThread):
    result = Signal(object, object)
    progress = Signal(float, str, str)
    def __init__(self, fn, *args):
        super().__init__()
        self.fn = fn
        self.args = args
    def run(self):
        try:
            out = self.fn(*self.args, progress_callback=self.progress.emit)
            self.result.emit('result', out)
        except Exception as e:
            tb = traceback.format_exc()
            self.result.emit('error', (str(e), tb))

def download_and_extract_zip(url, extract_to, *, progress_callback=None):
    tmp_zip = os.path.join(tempfile.gettempdir(), f"client_{now_str()}.zip")
    try:
        r = requests.get(url, stream=True, timeout=30)
        if not r.ok:
            raise Exception(f"HTTP error {r.status_code}: {r.reason}")
        total = int(r.headers.get("content-length", 0))
        written = 0
        with open(tmp_zip, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    written += len(chunk)
                    if progress_callback and total > 0:
                        percent = written / total * 100
                        progress_callback(percent, "Downloading...", "download")
        if not os.path.isfile(tmp_zip) or os.path.getsize(tmp_zip) < 128:
            raise Exception("Downloaded zip is missing or suspiciously small")
        with zipfile.ZipFile(tmp_zip, 'r') as zip_ref:
            nfiles = len(zip_ref.infolist())
            for idx, zinfo in enumerate(zip_ref.infolist()):
                try:
                    zip_ref.extract(zinfo, extract_to)
                except Exception as ex:
                    raise Exception(f"Failed extracting {zinfo.filename}: {ex}")
                if progress_callback and nfiles > 0:
                    percent = ((idx+1) / nfiles) * 100
                    progress_callback(percent, "Extracting...", "extract")
    except Exception as err:
        raise Exception("Download/extract failed: %s" % err)
    finally:
        try:
            if os.path.exists(tmp_zip):
                os.remove(tmp_zip)
        except Exception:
            pass
    if progress_callback:
        progress_callback(100, "Finished.", "extract")

class ProgressDialog(QDialog):
    def __init__(self, parent=None, title="Working...", allow_cancel=False):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setMinimumSize(500, 200)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint | Qt.WindowStaysOnTopHint)
        self.setWindowModality(Qt.ApplicationModal)
        self.setWindowIcon(QIcon(resource_path("app_icon.ico")))
        
        self._setup_ui(allow_cancel)
        
    def _setup_ui(self, allow_cancel):
        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(20, 20, 20, 20)
        
        self.status_label = QLabel("Initializing...")
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("%p%")
        self.progress_bar.setMinimumHeight(24)
        layout.addWidget(self.progress_bar)
        
        self.detail_label = QLabel("")
        self.detail_label.setWordWrap(True)
        layout.addWidget(self.detail_label)
        
        if allow_cancel:
            btn_layout = QHBoxLayout()
            self.cancel_button = QPushButton("Cancel")
            self.cancel_button.clicked.connect(self.reject)
            btn_layout.addStretch()
            btn_layout.addWidget(self.cancel_button)
            layout.addLayout(btn_layout)
    
    def update_progress(self, value, message):
        self.progress_bar.setValue(int(value))
        self.detail_label.setText(message)

class UpdaterApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BearMods")
        self.setMinimumSize(750, 525)
        self.setWindowIcon(QIcon(resource_path("app_icon.ico")))
        self.state = {
            "stage": "idle",
            "mods_path": None,
            "deletes": [],
            "adds": [],
            "replaces": [],
            "ignores": [],
            "backup_path": None,
            "error": None,
            "err_details": "",
            "progress_percent_download": 0.0,
            "progress_percent_extract": 0.0,
            "repo_dir": None,
            "repo_temp": None,
            "backup_before_apply": False,
        }
        self._in_progress_update = False
        self.worker = None
        self.progress_dialog = None
        self._init_fonts()
        self.reload_ui()

    def _init_fonts(self):
        self.font_h1 = QFont("Segoe UI", 20, QFont.Bold)
        self.font_h2 = QFont("Segoe UI", 14, QFont.Bold)
        self.font_small = QFont("Segoe UI", 10)

    def reload_ui(self):
        if self.layout():
            while self.layout().count():
                item = self.layout().takeAt(0)
                w = item.widget()
                if w:
                    w.deleteLater()
            lyt = self.layout()
        else:
            lyt = QVBoxLayout()
            self.setLayout(lyt)
        lyt.setSpacing(16)
        lyt.setContentsMargins(36, 24, 36, 24)
        if self.state["stage"] == "idle":
            lyt.addWidget(self._big_label("BearMods", color="#22baff"))
            lyt.addWidget(self._label("This tool will synchronize your Mods folder with Bear's repository, with backup and preview features."))
            lyt.addWidget(self._label("All actions require Administrator rights."))
            lyt.addSpacing(16)
            btn = QPushButton("Start")
            btn.setMinimumSize(220, 48)
            btn.setStyleSheet("QPushButton {font-size:17px;background-color: #39a7e9;color: white; border-radius: 8px;}")
            btn.clicked.connect(self.try_start)
            lyt.addWidget(btn, alignment=Qt.AlignmentFlag.AlignCenter)
        elif self.state["stage"] == "needpath":
            lyt.addWidget(self._big_label("Mods Directory Not Found!", color="#ee3338"))
            lyt.addWidget(self._label(f"Could not find:\n{DEFAULT_PATH}"))
            lyt.addWidget(self._label("Manually select your Mods folder."))
            h = QHBoxLayout()
            btn_pick = QPushButton("Pick Folder...")
            btn_pick.clicked.connect(self.pick_folder)
            btn_cancel = QPushButton("Exit")
            btn_cancel.clicked.connect(self.close)
            h.addWidget(btn_pick)
            h.addSpacing(18)
            h.addWidget(btn_cancel)
            lyt.addLayout(h)
        elif self.state["stage"] == "scanning":
            lyt.addWidget(self._big_label("Working...", color="#22baff"))
            lyt.addSpacing(10)
            lyt.addWidget(self._label("Downloading repository archive and indexing files..."))
            lyt.addSpacing(15)
            
            download_label = QLabel("Downloading:")
            download_label.setFont(self.font_small)
            download_label.setStyleSheet("color:#b5e7fa;")
            lyt.addWidget(download_label)
            
            self.progressbar_download = QProgressBar()
            self.progressbar_download.setRange(0, 100)
            self.progressbar_download.setValue(self.state.get("progress_percent_download", 0))
            self.progressbar_download.setFormat("%p%")
            self.progressbar_download.setMinimumHeight(22)
            lyt.addWidget(self.progressbar_download)
            
            extract_label = QLabel("Extracting:")
            extract_label.setFont(self.font_small)
            extract_label.setStyleSheet("color:#b5e7fa;")
            lyt.addWidget(extract_label)
            
            self.progressbar_extract = QProgressBar()
            self.progressbar_extract.setRange(0, 100)
            self.progressbar_extract.setValue(self.state.get("progress_percent_extract", 0))
            self.progressbar_extract.setFormat("%p%")
            self.progressbar_extract.setMinimumHeight(22)
            lyt.addWidget(self.progressbar_extract)
            
            lyt.addSpacing(4)
            self.progress_stage_label = QLabel(self.state.get("progress_stage", ""))
            self.progress_stage_label.setStyleSheet("color:#b5e7fa;font-size:11pt;")
            lyt.addWidget(self.progress_stage_label)
            self.run_thread(self._scan_all)
        elif self.state["stage"] == "summary":
            lyt.addWidget(self._big_label("Review Changes", color="#22baff"))
            lyt.addSpacing(2)
            h2 = QHBoxLayout()
            h2.addWidget(self.make_summary_group("Delete", self.state["deletes"], "#d33b3b"))
            h2.addWidget(self.make_summary_group("Add/Replace", self.state["adds"] + self.state["replaces"], "#1f953a"))
            h2.addWidget(self.make_summary_group("Ignore", self.state["ignores"], "#76839b"))
            lyt.addLayout(h2)
            lyt.addSpacing(6)
            
            button_row = QHBoxLayout()
            
            btn_exit = QPushButton("Exit")
            btn_exit.setMinimumWidth(90)
            btn_exit.clicked.connect(self.close)
            button_row.addWidget(btn_exit)
            
            button_row.addStretch()
            
            btn_backup = QPushButton("Backup Only")
            btn_backup.setMinimumWidth(120)
            btn_backup.clicked.connect(self.start_backup)
            button_row.addWidget(btn_backup)
            
            button_row.addStretch()
            
            btn_apply = QPushButton("Apply Changes!")
            btn_apply.setMinimumWidth(150)
            btn_apply.setStyleSheet("QPushButton {font-size: 15px; background: #18a155; color: white; border-radius: 5px;}")
            btn_apply.clicked.connect(self.start_apply)
            button_row.addWidget(btn_apply)
            
            lyt.addSpacing(8)
            lyt.addLayout(button_row)
        elif self.state["stage"] == "success":
            lyt.addWidget(self._big_label("Finished!", color="#22ff79"))
            lyt.addSpacing(12)
            bc = self.state["backup_path"] or ""
            backup_note = ""
            if bc:
                backup_note = f"\nBackup archive: {bc}"
            msg_text = f"All operations completed.{backup_note}"
            msg_lbl = QLabel(msg_text)
            msg_lbl.setFont(self.font_small)
            msg_lbl.setStyleSheet("color: #eafde8; padding: 7px;")
            msg_lbl.setWordWrap(True)
            msg_lbl.setMaximumWidth(650)
            lyt.addWidget(msg_lbl)
            lyt.addSpacing(16)
            btn = QPushButton("Exit")
            btn.setMinimumSize(110, 38)
            btn.setStyleSheet("font-size: 14px;")
            btn.clicked.connect(self.close)
            lyt.addSpacing(8)
            lyt.addWidget(btn, alignment=Qt.AlignmentFlag.AlignCenter)
        elif self.state["stage"] == "error":
            lyt.addWidget(self._big_label("Error!", color="#e82b50"))
            lyt.addSpacing(10)
            msg = self.state["error"] or "Unknown error."
            msg_lbl = QLabel(msg)
            msg_lbl.setFont(self.font_small)
            msg_lbl.setStyleSheet("color: #fab6b6; padding: 6px;")
            msg_lbl.setWordWrap(True)
            msg_lbl.setMaximumWidth(650)
            lyt.addWidget(msg_lbl)
            lyt.addSpacing(2)
            btn_row = QHBoxLayout()
            btn = QPushButton("Return")
            btn.setMinimumSize(110, 38)
            btn.setStyleSheet("font-size: 14px;")
            btn.clicked.connect(self.goto_idle)
            btn_row.addWidget(btn)
            if self.state.get("err_details"):
                self.details = QTextEdit(self.state["err_details"])
                self.details.setReadOnly(True)
                self.details.setMaximumHeight(0)
                self.details.setFont(QFont("Consolas", 9))
                show_btn = QPushButton("Show Details")
                def toggle_details():
                    if self.details.maximumHeight() < 20:
                        self.details.setMaximumHeight(220)
                        show_btn.setText("Hide Details")
                    else:
                        self.details.setMaximumHeight(0)
                        show_btn.setText("Show Details")
                show_btn.clicked.connect(toggle_details)
                btn_row.addWidget(show_btn)
            lyt.addLayout(btn_row)
            if self.state.get("err_details"):
                lyt.addWidget(self.details)

    def _label(self, text, font=None, color=None):
        lbl = QLabel(text)
        lbl.setFont(font or self.font_small)
        if color:
            lbl.setStyleSheet(f"color:{color}")
        lbl.setWordWrap(True)
        lbl.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        return lbl

    def _big_label(self, text, color="#0034a6"):
        lbl = QLabel(text)
        lbl.setFont(self.font_h1)
        lbl.setAlignment(Qt.AlignCenter)
        lbl.setStyleSheet(f"color: {color}; font-weight: bold;")
        lbl.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        return lbl

    def make_summary_group(self, title, filemetas, color):
        v = QVBoxLayout()
        lab = QLabel(f"{title} ({len(filemetas)})")
        lab.setFont(self.font_h2)
        lab.setStyleSheet(f"color:{color};")
        v.addWidget(lab)
        lst = QListWidget()
        lst.setMinimumWidth(220)
        lst.setMaximumWidth(245)
        lst.setMinimumHeight(180)
        lst.setMaximumHeight(260)
        for m in filemetas[:50]:
            item = QListWidgetItem(m.rel_path)
            lst.addItem(item)
        lst.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        v.addWidget(lst)
        wrap = QWidget()
        wrap.setLayout(v)
        wrap.setMinimumWidth(240)
        wrap.setMaximumHeight(312)
        return wrap

    def _set_stage(self, stage):
        self.state["stage"] = stage
        self.state["progress_percent_download"] = 0.0
        self.state["progress_percent_extract"] = 0.0
        self.state["progress_percent_backup"] = 0.0
        self.state["progress_percent_apply"] = 0.0
        self.state["progress_stage"] = ""
        self.reload_ui()

    def _show_error(self, msg, details=""):
        self.state["error"] = msg
        self.state["err_details"] = details
        self._set_stage("error")

    def goto_idle(self):
        repo_temp = self.state.get("repo_temp")
        if repo_temp:
            try:
                repo_temp.cleanup()
            except Exception:
                pass
        self.state = {
            "stage": "idle", 
            "mods_path": None, 
            "deletes": [], 
            "adds": [], 
            "replaces": [], 
            "ignores": [],
            "backup_path": None, 
            "error": None, 
            "err_details": "",
            "progress_percent_download": 0.0, 
            "progress_percent_extract": 0.0,
            "progress_percent_backup": 0.0, 
            "progress_percent_apply": 0.0,
            "progress_stage": "", 
            "repo_dir": None, 
            "repo_temp": None,
            "backup_before_apply": False
        }
        self.reload_ui()

    def try_start(self):
        try:
            if not os.path.exists(DEFAULT_PATH):
                self.state["stage"] = "needpath"
                self.reload_ui()
            else:
                self.state["mods_path"] = DEFAULT_PATH
                self._set_stage("scanning")
        except Exception as e:
            self._show_error(f"Startup error: {e}", traceback.format_exc())

    def pick_folder(self):
        try:
            d = QFileDialog.getExistingDirectory(self, "Select Mods Folder", str(Path.home()))
            if d:
                self.state["mods_path"] = d
                self._set_stage("scanning")
        except Exception as e:
            self._show_error(f"Folder selection failed: {e}", traceback.format_exc())

    def refresh_scan(self):
        repo_temp = self.state.get("repo_temp")
        if repo_temp:
            try:
                repo_temp.cleanup()
            except Exception:
                pass
        self.state["repo_dir"] = None
        self.state["repo_temp"] = None
        self._set_stage("scanning")

    def start_backup(self):
        try:
            self.progress_dialog = ProgressDialog(None, "Creating Backup")
            self.progress_dialog.status_label.setText("Creating a backup of your Mods folder...")
            self.progress_dialog.setWindowModality(Qt.ApplicationModal)
            self.progress_dialog.setModal(True)
            
            mods_path = self.state["mods_path"]
            
            self.worker = WorkerThread(self._do_backup, mods_path)
            self.worker.progress.connect(self.on_progress_dialog)
            self.worker.result.connect(self.on_backup_finish_dialog)
            self.worker.start()
            
            self.progress_dialog.exec()
        
        except Exception as e:
            if self.progress_dialog:
                self.progress_dialog.reject()
                self.progress_dialog = None
            self._show_error(f"Backup operation failed: {e}", traceback.format_exc())

    def start_apply(self):
        try:
            backup_msg = QMessageBox(self)
            backup_msg.setIcon(QMessageBox.Question)
            backup_msg.setWindowTitle("Create Backup?")
            backup_msg.setText("Would you like to create a zipped backup of your current Mods folder on your Desktop before making changes?")
            backup_msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel)
            backup_ret = backup_msg.exec()
            if backup_ret == QMessageBox.Cancel:
                return
                
            backup_before_apply = (backup_ret == QMessageBox.Yes)
            
            confirm_msg = QMessageBox(self)
            confirm_msg.setIcon(QMessageBox.Warning)
            confirm_msg.setWindowTitle("Confirm Changes")
            confirm_msg.setText(f"You are about to apply these changes to your Mods folder:\n"
                               f"• {len(self.state['deletes'])} items will be deleted\n"
                               f"• {len(self.state['adds']) + len(self.state['replaces'])} items will be added/updated\n\n"
                               f"Do you want to continue?")
            confirm_msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            confirm_ret = confirm_msg.exec()
            
            if confirm_ret == QMessageBox.No:
                return
                
            self.state["backup_before_apply"] = backup_before_apply
            
            self.progress_dialog = ProgressDialog(None, "Applying Changes")
            self.progress_dialog.status_label.setText("Applying changes to your Mods folder...")
            self.progress_dialog.setWindowModality(Qt.ApplicationModal)
            self.progress_dialog.setModal(True)
            
            mods_path = self.state["mods_path"]
            adds = self.state["adds"]
            deletes = self.state["deletes"]
            replaces = self.state["replaces"]
            repo_dir = self.state.get("repo_dir")
            backup_before_apply = self.state.get("backup_before_apply", False)
            
            self.worker = WorkerThread(self._do_apply, mods_path, adds, deletes, replaces, repo_dir, backup_before_apply)
            self.worker.progress.connect(self.on_progress_dialog)
            self.worker.result.connect(self.on_apply_finish)
            self.worker.start()
            
            self.progress_dialog.exec()
            
        except Exception as e:
            if self.progress_dialog:
                self.progress_dialog.reject()
                self.progress_dialog = None
            self._show_error(f"Operation failed: {e}", traceback.format_exc())

    def run_thread(self, fn):
        try:
            if self.worker and self.worker.isRunning():
                return
            if fn == self._scan_all:
                mods_path = self.state["mods_path"]
                self.worker = WorkerThread(self._scan_all, mods_path)
                self.worker.progress.connect(self.on_progress)
                self.worker.result.connect(self.on_scan_finish)
                self.worker.start()
            elif fn == self._do_backup:
                mods_path = self.state["mods_path"]
                self.worker = WorkerThread(self._do_backup, mods_path)
                self.worker.progress.connect(self.on_progress)
                self.worker.result.connect(self.on_backup_finish)
                self.worker.start()
        except Exception as e:
            self._show_error(f"Background operation error: {e}", traceback.format_exc())

    @Slot(float, str, str)
    def on_progress(self, percent, stage, progress_type="default"):
        if self._in_progress_update:
            return
            
        self._in_progress_update = True
        try:
            if progress_type == "download":
                self.state["progress_percent_download"] = percent
                if hasattr(self, "progressbar_download"):
                    self.progressbar_download.setValue(int(percent))
            elif progress_type == "extract":
                self.state["progress_percent_extract"] = percent
                if hasattr(self, "progressbar_extract"):
                    self.progressbar_extract.setValue(int(percent))
            elif self.state["stage"] == "backup":
                self.state["progress_percent_backup"] = percent
                if hasattr(self, "progressbar"):
                    self.progressbar.setValue(int(percent))
            
            self.state["progress_stage"] = stage
            if hasattr(self, "progress_stage_label"):
                self.progress_stage_label.setText(stage)
                
            QApplication.processEvents()
        finally:
            self._in_progress_update = False

    @Slot(float, str, str)
    def on_progress_dialog(self, percent, stage, progress_type="default"):
        if self._in_progress_update:
            return
            
        self._in_progress_update = True
        try:
            if self.progress_dialog:
                self.progress_dialog.update_progress(percent, stage)
                QApplication.processEvents()
        finally:
            self._in_progress_update = False

    def on_scan_finish(self, typ, data):
        if typ == 'error':
            msg, tb = data
            self._show_error(msg, tb)
        else:
            (deletes, adds, replaces, ignores, repo_dir), repo_temp = data
            self.state["deletes"] = deletes
            self.state["adds"] = adds
            self.state["replaces"] = replaces
            self.state["ignores"] = ignores
            self.state["repo_dir"] = repo_dir
            self.state["repo_temp"] = repo_temp
            self._set_stage("summary")

    def on_backup_finish(self, typ, data):
        if typ == 'error':
            msg, tb = data
            self._show_error(msg, tb)
        else:
            self.state["backup_path"] = data
            self._set_stage("success")

    def on_backup_finish_dialog(self, typ, data):
        if self.progress_dialog:
            self.progress_dialog.accept()
            self.progress_dialog = None
        
        if typ == 'error':
            msg, tb = data
            self._show_error(msg, tb)
        else:
            self.state["backup_path"] = data
            
            backup_msg = QMessageBox(self)
            backup_msg.setIcon(QMessageBox.Information)
            backup_msg.setWindowTitle("Backup Complete")
            backup_msg.setText(f"Backup completed successfully!\n\nBackup saved to:\n{data}")
            backup_msg.setStandardButtons(QMessageBox.Ok)
            backup_msg.exec()

    def on_apply_finish(self, typ, data):
        if self.progress_dialog:
            self.progress_dialog.accept()
            self.progress_dialog = None
        
        if typ == 'error':
            msg, tb = data
            self._show_error(msg, tb)
        else:
            self.state["backup_path"] = data
            
            complete_msg = QMessageBox(self)
            complete_msg.setIcon(QMessageBox.Information)
            complete_msg.setWindowTitle("Operation Complete")
            
            backup_note = ""
            if data:
                backup_note = f"\n\nA backup was created at:\n{data}"
                
            complete_msg.setText(f"All changes have been successfully applied!{backup_note}\n\nDo you want to exit now?")
            complete_msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            exit_ret = complete_msg.exec()
            
            if exit_ret == QMessageBox.Yes:
                self.close()
            else:
                self._set_stage("success")

    def _scan_all(self, mods_path, *, progress_callback):
        try:
            temp_dir = tempfile.TemporaryDirectory(prefix="mod_temp_repo")
            repo_path = Path(temp_dir.name)
            download_and_extract_zip(CDN_URL, repo_path, progress_callback=progress_callback)
            progress_callback(100, "Scanning files...", "extract")
            mod_map = scan_dir_with_hashes(mods_path, True)
            repo_map = scan_dir_with_hashes(repo_path, False)
            deletes, adds, replaces, ignores = [], [], [], []
            for mfp, mmeta in mod_map.items():
                if mfp not in repo_map:
                    deletes.append(FileMeta(mfp, mmeta["is_dir"]))
            for rfp, rmeta in repo_map.items():
                if rfp not in mod_map:
                    adds.append(FileMeta(rfp, rmeta["is_dir"]))
            for rfp, rmeta in repo_map.items():
                if rfp in mod_map:
                    mmeta = mod_map[rfp]
                    if not rmeta["is_dir"] and rmeta["hash"] != mmeta["hash"]:
                        replaces.append(FileMeta(rfp, False))
                    elif rmeta["is_dir"] == mmeta["is_dir"]:
                        ignores.append(FileMeta(rfp, rmeta["is_dir"]))
            progress_callback(100, "Done.", "extract")
            return (deletes, adds, replaces, ignores, str(repo_path)), temp_dir
        except Exception as e:
            raise Exception(f"Repository fetch/scanning failed: {e}")

    def _do_backup(self, mods_path, *, progress_callback):
        try:
            progress_callback(0, "Starting backup...", "backup")
            backup_name = f"{BACKUP_PREFIX}{now_str()}"
            backup_path = Path(user_desktop()) / backup_name
            
            def copy_progress(percent, msg):
                progress_callback(percent * 0.7, msg, "backup")
                
            copy_dir_all(mods_path, backup_path, progress_callback=copy_progress)
            
            zip_path = str(backup_path) + ".zip"
            
            def zip_progress(percent, msg):
                progress_callback(70 + percent * 0.3, msg, "backup")
                
            create_zip_backup(str(backup_path), zip_path, progress_callback=zip_progress)
            
            shutil.rmtree(backup_path)
            
            progress_callback(100, f"Backup complete: {zip_path}", "backup")
            return zip_path
        except Exception as e:
            raise Exception(f"Backup failed: {e}")

    def _do_apply(self, mods_path, adds, deletes, replaces, repo_dir, backup_before_apply=False, *, progress_callback):
        backup_path = None
        try:
            if backup_before_apply:
                progress_callback(0, "Starting backup before apply...", "apply")
                backup_name = f"{BACKUP_PREFIX}{now_str()}"
                backup_dir = Path(user_desktop()) / backup_name
                
                def copy_progress(percent, msg):
                    progress_callback(percent * 0.6, msg, "apply")
                    
                copy_dir_all(mods_path, backup_dir, progress_callback=copy_progress)
                
                zip_path = str(backup_dir) + ".zip"
                
                def zip_progress(percent, msg):
                    progress_callback(60 + percent * 0.2, msg, "apply")
                    
                create_zip_backup(str(backup_dir), zip_path, progress_callback=zip_progress)
                
                shutil.rmtree(backup_dir)
                backup_path = zip_path
                progress_callback(20, "Backup complete.", "apply")
            
            repo_path = Path(repo_dir)
            total = len(deletes) + len(adds) + len(replaces)
            done = 0
            
            for meta in deletes:
                target = Path(mods_path) / meta.rel_path
                if target.exists():
                    try:
                        if meta.is_dir:
                            if os.path.isdir(target):
                                for root, dirs, files in os.walk(target, topdown=False):
                                    for name in files:
                                        file_path = os.path.join(root, name)
                                        os.chmod(file_path, 0o777)
                                        os.remove(file_path)
                                    for name in dirs:
                                        dir_path = os.path.join(root, name)
                                        os.rmdir(dir_path)
                                os.rmdir(target)
                        else:
                            if os.path.isfile(target):
                                os.chmod(target, 0o777)
                                os.remove(target)
                    except Exception as ex:
                        pass
                done += 1
                if total > 0:
                    percent = 20 + (done / total) * 70
                    progress_callback(percent, f"Deleting: {meta.rel_path}", "apply")
            
            for meta in adds + replaces:
                src = repo_path / meta.rel_path
                dst = Path(mods_path) / meta.rel_path
                try:
                    if meta.is_dir:
                        if not dst.exists():
                            os.makedirs(dst)
                    elif src.exists():
                        if not dst.parent.exists():
                            os.makedirs(dst.parent)
                        if os.path.exists(dst):
                            os.chmod(dst, 0o777)
                        shutil.copy2(src, dst)
                except Exception as ex:
                    raise Exception(f"Copy failed for {src} to {dst}: {ex}")
                done += 1
                if total > 0:
                    percent = 20 + (done / total) * 70
                    progress_callback(percent, f"Copying: {meta.rel_path}", "apply")
            
            progress_callback(100, "Done.", "apply")
            return str(backup_path) if backup_path else ""
        except Exception as e:
            raise Exception(f"Failed applying changes: {e}")

def setAppDarkPalette(app):
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(40, 40, 40))
    palette.setColor(QPalette.WindowText, Qt.white)
    palette.setColor(QPalette.Base, QColor(30, 30, 30))
    palette.setColor(QPalette.AlternateBase, QColor(53, 53, 53))
    palette.setColor(QPalette.ToolTipBase, Qt.white)
    palette.setColor(QPalette.ToolTipText, Qt.white)
    palette.setColor(QPalette.Text, Qt.white)
    palette.setColor(QPalette.Button, QColor(53, 53, 53))
    palette.setColor(QPalette.ButtonText, Qt.white)
    palette.setColor(QPalette.BrightText, Qt.red)
    palette.setColor(QPalette.Highlight, QColor(42, 130, 218))
    palette.setColor(QPalette.HighlightedText, Qt.black)
    app.setPalette(palette)

def main():
    try:
        app = QApplication(sys.argv)
        app.setStyle("Fusion")
        app_icon = QIcon(resource_path("app_icon.ico"))
        app.setWindowIcon(app_icon)
        setAppDarkPalette(app)
        updater = UpdaterApp()
        updater.show()
        sys.exit(app.exec())
    except Exception:
        excepthook(*sys.exc_info())

if __name__ == "__main__":
    main()
