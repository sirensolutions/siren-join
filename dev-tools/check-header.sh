#! /bin/bash

###
### Check file headers
###

# Get the current directory
DIR=$(cd $(dirname "$0"); pwd)

# Get the target directory
TARGET_DIR="$DIR/../"

# AGPL copyright header
COPYRIGHT_AGPL=$(cat $DIR/copyright_agpl | sed 's/\//\\\//g')

# check if an element is in an array - usage: array_contains(array, element)
function array_contains () {
  array=$1[@]
  seeking=$2
  a=("${!array}")

  for element in "${a[@]}"; do
      if [[ "${seeking}" =~ .*${element}$ ]]; then
          return 0
      fi
  done

  return 1
}

function change_header {
  # Get the files: *.java" but exclude the "package-info.java" files
  FILES=$(find $1/src/ -type f \( \( -name "*.java" -o -name "*.flex" \) -and ! -name "package-info.java" \))

  for file in $FILES; do
    # Find the first ending comment
    end_com=$(grep -n -m 1 "\*/$" $file | cut -f 1 -d ':')
    # Check if the head part to the line $end_com contains Copyright
    head -n $end_com $file | grep " Copyright " > /dev/null

    if [[ $? -eq 0 ]]; then
      # Replace the copyright header with the default one
      perl -0777 -i -pe 's/^\/\*\*(?:(?!\*\/$).)*\*\/$/'"$2"'/ism' $file
    else # Missing header, probably
      perl -0777 -i -pe 's/^/'"$2"'\n/ism' $file
    fi
  done

  # Remove comments with either @project, @email and @author javadocs
  for file in $FILES; do
    perl -0777 -i -pe 's/^\/\*\*(?:(?!\*\/$).)*@(project|email)(?:(?!\*\/$).)*\*\/$//ism' $file
    sed -i '/@author/d' $file
  done
}

change_header "${TARGET_DIR}" "${COPYRIGHT_AGPL}"
