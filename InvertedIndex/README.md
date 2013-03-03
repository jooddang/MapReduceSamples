InvertedIndex
=============

build inverted index for basic search engine


XmlInputFormat.java is an input formatter to parse xml input.
From BuildIndex.java, Mapper builds inverted index, and Reducer prints line by line.

After Export to *.jar, run
$ hadoop jar buildindex.jar BuildIndex /corpus /user/st01/output
on the shell.
