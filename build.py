import PyInstaller.__main__
import os
import shutil

if os.path.exists("dist"):
    shutil.rmtree("dist")
if os.path.exists("build"):
    shutil.rmtree("build")

PyInstaller.__main__.run([
    '--name=BearMods',
    '--onefile',
    '--windowed',
    '--icon=app_icon.ico',
    '--add-data=app_icon.ico;.',
    '--hidden-import=xxhash',
    'app.py'
])

print("Build complete! Executable is in the dist folder.")
