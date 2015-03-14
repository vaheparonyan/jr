DROP TABLE sandbox.kb_ks_locale_city;

SQL_BREAK;

CREATE SET TABLE sandbox.kb_ks_locale_city ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      locale VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
      city VARCHAR(36) CHARACTER SET UNICODE NOT CASESPECIFIC)
PRIMARY INDEX ( locale ,city );

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_GB','Derby');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_GB','Belfast');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_GB','Liverpool');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_GB','Birmingham');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_GB','Edinburgh');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_GB','Leeds');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_GB','Manchester');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_GB','Glasgow');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_GB','London');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_GB','Cardiff');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('fr_FR','Paris');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('fr_FR','Lyon');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('fr_FR','Marseille');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('fr_FR','Bordeaux');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('fr_FR','Toulouse');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('fr_FR','Nantes');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('fr_FR','Nice');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('fr_FR','Lille');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('fr_FR','Strasbourg');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('fr_FR','Montpellier');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('de_DE','Berlin');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('de_DE','München');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('de_DE','Hamburg');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('de_DE','Köln');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('de_DE','Frankfurt');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('de_DE','Düsseldorf');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('de_DE','Leipzig');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('de_DE','Dresden');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('de_DE','Stuttgart');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('de_DE','Hannover');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('es_ES','Madrid');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('es_ES','Barcelona');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('es_ES','Valencia');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('es_ES','Zaragoza');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('es_ES','Malaga');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('es_ES','Granada');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('es_ES','Palma de Mallorca');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('es_ES','Sevilla');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('es_ES','Cordoba');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('es_ES','Bilbao');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('it_IT','Roma');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('it_IT','Milano');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('it_IT','Torino');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('it_IT','Napoli');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('it_IT','Bari');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('it_IT','Bologna');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('it_IT','Firenze');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('it_IT','Venezia');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('it_IT','Catania');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('it_IT','Brescia');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Atlanta');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Austin');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Boston');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Chicago');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Colorado Springs');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Dallas');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Denver');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Houston');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Las Vegas');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Los Angeles');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Milwaukee');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','New York');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Milwaukee');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Orlando');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Philadelphia');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Phoenix');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','San Antonio');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','San Antonio');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','San Diego');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','San Francisco');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Seattle');

SQL_BREAK;

insert into sandbox.kb_ks_locale_city values('en_US','Toledo');
