# 1brc - the one billion row challenge

This is my effort at the challenge using Go. The key features are:

* Read the file in chunks and process the chunks in parallel to make use of multiple CPUs
* Build the results using a trie, there will be one trie per chunk
* Merge the chunk tries into a final result trie
* Dump the final results to the console