#!/bin/sh

APP_NAME=guardian
VERSION=1.0.0
PACKAGE_DIR=$APP_NAME"_"$VERSION
echo "Building Package $PACKAGE_DIR"

pyinstaller --onefile $APP_NAME.py
if [ $? != 0 ];then
    echo "[ERROR] failed to build $APP_NAME module: $APP_NAME"
    exit -1
fi

pyinstaller --onefile alert.py
if [ $? != 0 ];then
    echo "[ERROR] failed to build $APP_NAME module: alert"
    exit -1
fi

cd dist
mkdir -p $PACKAGE_DIR/bin
cp $APP_NAME $PACKAGE_DIR/bin
cp alert $PACKAGE_DIR/bin

mkdir -p $PACKAGE_DIR/config
cp ../config.json.template $PACKAGE_DIR/config

tar zcvf $PACKAGE_DIR.tar.gz $PACKAGE_DIR
