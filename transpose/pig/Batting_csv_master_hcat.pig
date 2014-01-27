batting = load 'Batting.csv' using PigStorage(',')
AS (playerid:chararray, year:int, stint:int, teamid:chararray, lgid:int, g:int, g_batting:int, ab:int, runs:int);

runs = FILTER batting BY runs > 0;

master = LOAD 'master' using org.apache.hcatalog.pig.HCatLoader();

c = JOIN batting BY playerid, master by playerid;

dump c;
