#!/usr/bin/env bash

echo "determining version for $GITHUB_REF"
if [[ "${GITHUB_REF#refs/tags/}" =~ ^${VERSION_PATTERN}$ ]]; then
  export rev=${GITHUB_REF#refs/tags/}
  export sha1=""
  export changelist=""
else
  export gh_ref=${GITHUB_REF#refs/*/}
  export branch=${gh_ref/\//-}
  if [[ $branch =~ ^[1-9]*-merge$ ]]; then
    export branch=PR${branch%-merge}
  fi
  export lasttag=`git describe --tags --match=${VERSION_PATTERN} --abbrev=0`
  export desc=`git describe --tags --match=${VERSION_PATTERN} --long`
  if [[ -z "$lasttag" ]]; then
    echo "[warning] Could not find last valid version number!"
    export lasttag=0.0.1
    export newcommits=-0
  else
    export tmp=${desc#"$lasttag"}
    export newcommits=${tmp%-*}
  fi
  export rev=${lasttag}-${branch}${newcommits}
  export sha1=-$(git rev-parse --short HEAD)
  export changelist="-SNAPSHOT"
fi
echo "revision=$rev" >> "$GITHUB_OUTPUT"
echo "sha1=$sha1" >> "$GITHUB_OUTPUT"
echo "changelist=$changelist" >> "$GITHUB_OUTPUT"
echo "Version will be:"
echo "${rev}${sha1}${changelist}"
