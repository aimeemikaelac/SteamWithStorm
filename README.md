# SteamWithStorm

A Storm topology that can crawl Steam servers to attempt to determine the most played games by sampling.

Note: all the required libraries have to be included in specific folders holding .class files so that a jar that is compatible with Storm can be created.

Libraries that are used, beyond the Storm API, are limited to JFreeChart for the GUI (http://www.jfree.org/jfreechart/) and the Simple JSON Parser available at: https://code.google.com/p/json-simple/

Other libraries include Java Servlets and JUnit but are not used at this time.

Note: I have revoked the hard-codes Steam API key that can be seen in this code.
