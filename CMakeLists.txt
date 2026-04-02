cmake_minimum_required(VERSION 3.16)
 
include($ENV{IDF_PATH}/tools/cmake/project.cmake)
 
project(local_gateway_gw1)
 


Remove-Item -Recurse -Force build
Remove-Item -Recurse -Force main\build -ErrorAction SilentlyContinue

python -m esptool --chip esp32 -p COM5 erase_flash

idf.py erase-flash
idf.py fullclean

idf.py set-target esp32
idf.py build