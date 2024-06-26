# python-jamfsync
jamfsync is a Python 3 module for accessing the Jamf School APIv1. The APIv1 is the primary tool for programmatically accessing data on a Jamf School system to enable integrations with other utilities or systems. 
The concept behind this is to have a class or simply a collection of data (variables) and methods (functions) that map directly to the API (https://example.jamfcloud.com/api/).
The jamfsync class takes care of the URL requests, authentication and conversion of XML/JSON to Python dictionaries/lists. These are then converted into a Pandas DataFrame and returned as directly accessable variables.

The primary goal of the class is to transfer the user and group data from the school server IServ to the jamf School cloud and also to create classes in Jamf School, whereby a distinction is made between students and teachers. 
The distinction is based on a group that uniquely identifies the teachers on the IServ. The default group is "Lehrkreafte", which can be modified.
