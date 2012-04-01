

rem **************************************
rem Generated file, do not modify!!!!
rem
rem
rem **************************************

@echo off
if "%JAVA_HOME%" == "" goto noJavaHome

set ANT_HOME=%CD%\tools\ant

tools\ant\bin\ant -f bldfiles\build.xml %1 %2 %3 %4 %5 %6 %7 %8 %9

goto end

:noJavaHome
if "%_JAVACMD%" == "" set _JAVACMD=java
echo.
echo Error: JAVA_HOME environment variable is not set.
echo   It must be set to the jdk1.4 directory
echo.

:end
pause
