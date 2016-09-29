Lighthouse is a simple jsonrpc fuzzy-string-comparision based search engine for publications on the lbrycrd blockchain.

**Installation**

-Clone this repository

-Run `sudo python setup.py install`

-While running lbrynet-daemon, start lighthouse with `start-lighthouse`. 

It is recommended to use lbrynet-daemon with
the lbrycrd wallet. During first run lighthouse will populate a name database, this can take a little while. The configuration file is located at ~/.lighthouse.yml, if it doesn't exist lighthouse will generate it with default settings.

**Testing**

This server is run by lighthouse1.lbry.io, lighthouse2.lbry.io, and lighthouse3.lbry.io. The default port lighthouse uses is 50005.

To interact with lighthouse from a python terminal, run the following:


 `from jsonrpc.proxy import JSONRPCProxy`
 
 `lh = JSONRPCProxy.from_url("http://lighthouse1.lbry.io:50005")`
 
 `results = lh.search("test search")`
 
 `print "Got %i search results" % len(results)`
