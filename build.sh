#!/bin/bash

# Aborta o script se algum comando falhar
set -e

# Define o modo de compilação (Padrão: release se nenhum argumento for passado)
MODE=${1:-release}

mkdir -p build

COMMON_FLAGS="-Iinclude $(pkg-config --cflags fuse3) -std=c++20 -Wall -Wextra"
LIBS="$(pkg-config --libs fuse3) -luring -llz4 -lpthread"

if [ "$MODE" == "debug" ]; then
    echo "[BUILD] Compilando RISE em modo DEBUG (Logs e depuração ativados)..."
    CXXFLAGS="$COMMON_FLAGS -g -O0 -DENABLE_DEBUG"
elif [ "$MODE" == "release" ]; then
    echo "[BUILD] Compilando RISE em modo RELEASE (Desempenho máximo -O3)..."
    CXXFLAGS="$COMMON_FLAGS -O3 -flto -march=native"
else
    echo "Uso: ./build.sh [release|debug]"
    exit 1
fi

g++ \
src/*.cpp \
src/*/*.cpp \
$CXXFLAGS \
$LIBS \
-o build/RISE

echo "[BUILD] Build concluído com sucesso em build/RISE!"
