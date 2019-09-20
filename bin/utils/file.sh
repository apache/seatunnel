#!/usr/bin/env bash
# List all files in specific dir and concat full path of files by specific delimeter.
function listFiles {

    dir=$1
    delimeter=$2
    ignore_file_regex=${3:-""} # if $3 not set, set default value: empty string("")

    jar_list=""
    for file in $(ls $dir); do

        if [ "x$ignore_file_regex" != "x" ]; then

            if [[ $file =~ $ignore_file_regex ]]; then
                # if file match ignore file regex,
                # then don't include this file.
                continue
            fi
        fi

        jar_file=$dir/$file
        if [ "x$jar_list" == "x" ]; then
            jar_list=$jar_file
        else
            jar_list="${jar_list}${delimeter}${jar_file}"
        fi
    done

    echo $jar_list
}

function listJars {
    dir=$1
    echo $(listFiles $dir ",")
}

## list jars dependencies of plugins
function listJarDependenciesOfPlugins {
    dir=$1
    allJars=""
    for plugin_dir in $(ls $dir); do
        abs_plugin_dir=$dir/$plugin_dir
        for subdir in $(ls $abs_plugin_dir); do
            abs_subdir=$abs_plugin_dir/$subdir
            jars=""
            if [ "$subdir" == "lib" ]; then
                jars=$(listJars $abs_subdir)

                if [ "x$allJars" == "x" ]; then
                    allJars=$jars
                else
                    allJars="${allJars},${jars}"
                fi
            fi
        done
    done

    echo $allJars
}
