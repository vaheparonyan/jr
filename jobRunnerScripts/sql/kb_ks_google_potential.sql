CREATE SET TABLE sandbox.kb_ks_google_potential ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      locale VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
      keyword_string VARCHAR(1048) CHARACTER SET UNICODE NOT CASESPECIFIC,
      keyword_returned VARCHAR(1048) CHARACTER SET UNICODE NOT CASESPECIFIC,
      search_volume INTEGER,
      average_cpc DECIMAL(5,2),
      competition DECIMAL(5,2))
PRIMARY INDEX ( locale ,keyword_string );
