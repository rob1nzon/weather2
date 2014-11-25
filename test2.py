import sys
from xml.dom.minidom import *
if len (sys.argv) > 1:
    xml = parse(sys.argv[1])
else:
    xml = parse('output.txt')
name = xml.getElementsByTagName('w')[0].getElementsByTagName('ana')
for node in name:
    print node.nodeValue