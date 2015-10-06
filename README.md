CSVInputFormat
==============

CSV input format for Hadoop. Handles delimited cells that contain separators, newlines and UTF-8 encoding. Delimiters in
a delimited string should be represented as 2 of the characters (this library will replace the double delimiter with a
single character in the output).

CSVInputFormat will split automatically using an algorithm similar to FileInputFormat. CSVNLineInputFormat will split
based on a configurable number of lines.

The output key is the position in the file where the row begins and the output value is a list of strings that contain
each cells content.

Configuration supports custom separator and delimiter characters and if using the CSVNLineInputFormat, custom number of
lines to split on.

More ideas to improve this are welcome.

Example:
--------------------------------------------------------------------------------
If we read this CSV (note that line 2 is multiline):

	Joe Demo,"2 Demo Street,
	Demoville,
	Australia. 2615",joe@someaddress.com
	Jim Sample,"3 Sample Street, Sampleville, Australia. 2615",jim@sample.com
	Jack Example,"1 Example Street, Exampleville, Australia.
	2615",jack@example.com


The output is as follows:

	==> TestMapper
	==> key=0
	==> val[0] = Joe Demo
	==> val[1] = 2 Demo Street, 
	Demoville, 
	Australia. 261
	==> val[2] = joe@someaddress.com
	
	==> TestMapper
	==> key=73
	==> val[0] = Jim Sample
	==> val[1] = 
	==> val[2] = jim@sample.com

	==> TestMapper
	==> key=100
	==> val[0] = Jack Example
	==> val[1] = 1 Example Street, Exampleville, Australia. 261
	==> val[2] = jack@example.com