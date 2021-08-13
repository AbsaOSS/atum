@ECHO OFF

:: Copyright 2018 ABSA Group Limited

:: Licensed under the Apache License, Version 2.0 (the "License");
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
::     http://www.apache.org/licenses/LICENSE-2.0

:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.

SETLOCAL EnableDelayedExpansion

SET STATUS=0
CALL mvn clean
IF %ERRORLEVEL% NEQ 0 GOTO end

CALL :cross_build 2.11 2.4
IF %ERRORLEVEL% NEQ 0 GOTO end
CALL :cross_build 2.12 2.4
IF %ERRORLEVEL% NEQ 0 GOTO end
CALL :cross_build 2.12 3.1
IF %ERRORLEVEL% NEQ 0 GOTO end

ECHO ===============================================================================
ECHO Restoring version
ECHO ===============================================================================
CALL mvn scala-cross-build:restore-version

:end
EXIT /B %ERRORLEVEL%

:cross_build
    SET SCALA_VER=%~1
    SET SPARK_VER=%~2
    ECHO ===============================================================================
    ECHO Building with Scala %SCALA_VER% for Spark %SPARK_VER%
    ECHO ===============================================================================
    CALL mvn scala-cross-build:change-version -Pscala-%SCALA_VER%
    CALL mvn clean install -Pspark-%SPARK_VER%
EXIT /B %ERRORLEVEL%
