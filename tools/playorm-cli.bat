@echo off

if "%OS%" == "Windows_NT" setlocal

if NOT DEFINED JAVA_HOME goto :err

REM Ensure that any user defined CLASSPATH variables are not used on startup
set CLASSPATH=

REM For each jar in the lib directory call append to build the CLASSPATH variable.
for %%i in ("..\output\jardist\*.jar") do call :append "%%i"
for %%j in ("..\lib\*.jar") do call :append "%%j"

goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

:okClasspath
REM Include the \classes directory so it works in development
set PLAYORM_CLASSPATH="..\bin";%CLASSPATH%;
goto runCli

:runCli
"%JAVA_HOME%\bin\java" -cp %PLAYORM_CLASSPATH% com.alvazan.ssql.cmdline.PlayOrm %*
goto finally

:err
echo The JAVA_HOME environment variable must be set to run this program!
pause

:finally

ENDLOCAL
