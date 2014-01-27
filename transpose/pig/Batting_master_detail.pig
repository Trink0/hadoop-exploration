ibatting = load 'batting_data' using org.apache.hcatalog.pig.HCatLoader();

b = LOAD 'master' using org.apache.hcatalog.pig.HCatLoader();

c = JOIN batting BY playerid, b by playerid;

dump c;
