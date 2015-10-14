# SIREn Join Build Instructions

Basic steps:

 1. Install JDK 1.7 (or greater), Maven 2.0.9 (or greater)
 2. Download SIREn Join and unpack it
 3. Connect to the directory of your SIREn Join installation
 5. Build the binary distribution

## Set up your development environment (JDK 1.7 or greater, Maven 2.0.9 or greater)

We'll assume that you know how to get and set up the JDK - if you
don't, then we suggest starting at http://java.sun.com and learning
more about Java, before returning to this BUILD document.

KnowledgeBrowser uses Apache Maven for build control. Specifically, you MUST use Maven
version 2.0.9 or greater.

## Download SIREn Join

Download the tarred/gzipped version of the archive, and uncompress it into a
directory of your choice.

## From the command line, change (cd) into the directory of your SIREn Join installation

SIREn Join's installation directory contains the project pom.xml file. By default,
you do not need to change any of the settings in this file, but you do
need to run maven from this location so it knows where to find pom.xml.

--------------------------------------------------------------------------------

Copyright (c) 2015, SIREn Solutions. All Rights Reserved.
