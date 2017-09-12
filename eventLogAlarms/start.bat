set /p filepath=Enter the log file path:

echo path


start python %~dp0main.py -p %filepath%

pause