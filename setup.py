from cx_Freeze import setup, Executable

setup(
    name = "21",
    version = "0.1",
    description = "Les",
    executables = [Executable("main.py")]
)