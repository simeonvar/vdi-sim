#!/usr/bin/env python

import ConfigParser
# a simple simulator program 

SETTING = "./setting.conf"

def main():
    Config = ConfigParser.ConfigParser()
    Config.read(SETTING)
    dict1={}
    for section in Config.sections():
        #print section
        dict1 = dict(dict1.items() + Config.items(section))
    print dict1
main()
