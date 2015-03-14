#!/bin/sh
datafilename=$1
datekey=$2
separator="||||"

echo Running $0 $1 $2
#temporary workaround, the host should be configured correctly
export LD_LIBRARY_PATH=/packages/encap/gcc-4.5.2/lib/:$LD_LIBRARY_PATH

sed -e ':a;s/^\(\("[^"]*"\|[^",]*\)*\),/\1||||/;ta' ./csv/${datafilename} > ./csv/.${datafilename}
echo New file is created for ./csv/${datafilename}. The \',\' separator is changed to \'${separator}\'. 

fastload << !!END!!

SESSIONS 4;

ERRLIMIT 25;

.LOGON 10.20.88.35/vparonyan,LetsDoTDWC12

drop table sandbox.Error1;
drop table sandbox.Error2;

create table sandbox.temp_KBR_${datekey}
    (Keyword     VARCHAR(100),
     URL         VARCHAR(600),
     Pos         VARCHAR(100),
     Trend       VARCHAR(100),
     Tags        VARCHAR(100),
     Ttl         VARCHAR(200),
     USI         VARCHAR(100),
     Volume      VARCHAR(100),
     Traffic     VARCHAR(100),
     CPC         VARCHAR(100),
     Dt          VARCHAR(50))
     PRIMARY INDEX(Keyword, Pos, Ttl, Tags, Dt);
                                     
 BEGIN LOADING sandbox.temp_KBR_${datekey} ERRORFILES sandbox.Error1, sandbox.Error2
    CHECKPOINT 10000;
    .RECORD 2;
    .SET RECORD VARTEXT "${separator}";
   
    DEFINE   Keyword     (VARCHAR(100)),
             URL         (VARCHAR(600)),
             Pos         (VARCHAR(100)),
             Trend       (VARCHAR(100)),
             Tags        (VARCHAR(100)),
             Ttl         (VARCHAR(200)),
             USI         (VARCHAR(100)),
             Volume      (VARCHAR(100)),
             Traffic     (VARCHAR(100)),
             CPC         (VARCHAR(100)),
             Dt          (VARCHAR(50))
    FILE = ./csv/.${datafilename};

    INSERT INTO sandbox.temp_KBR_${datekey} (Keyword, URL, Pos, Tags, Ttl, USI, Volume, Traffic, CPC, Dt) VALUES 
        (:Keyword, :URL, :Pos, :Tags, :Ttl, :USI, :Volume, :Traffic, :CPC, '${datekey}');

END LOADING;


.LOGOFF;


!!END!!
