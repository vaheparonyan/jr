DROP TABLE sandbox.kb_taxonomy_updates;

SQL_BREAK;

CREATE MULTISET TABLE sandbox.kb_taxonomy_updates ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      locale VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
      guid VARCHAR(36) CHARACTER SET UNICODE CASESPECIFIC,
      friendly_name_title VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC)
PRIMARY INDEX ( locale, guid, friendly_name_title );

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('en_GB','c4788e6b-2a57-4a51-bc86-b2cf4f78108a','Theatres');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('en_GB','c4788e6b-2a57-4a51-bc86-b2cf4f78108a','Shows');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('en_GB','f1ed1965-b3ad-4b08-a6b5-fd12714d5eb9','Health Shopping');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('en_GB','f1ed1965-b3ad-4b08-a6b5-fd12714d5eb9','Beauty Shopping');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('en_GB','7fef3478-e7da-4fbe-b015-e6d240558e5e','Tickets');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('en_GB','7fef3478-e7da-4fbe-b015-e6d240558e5e','Events');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('en_GB','294ea30b-dd37-49a1-9805-e9e6c7617902','Beauty');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('en_GB','294ea30b-dd37-49a1-9805-e9e6c7617902','Spas');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('en_GB','f66acf4e-ed76-4d0b-ae71-0449a7cd3457','Home and Garden');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('en_GB','f386db7c-919e-4762-8f9b-c906c7e101f0','Skiing');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('en_GB','f386db7c-919e-4762-8f9b-c906c7e101f0','Snowboarding');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('en_GB','1d73e2c7-995a-4e27-a424-734e366888f7','Fitness');

SQL_BREAK;

delete from sandbox.kb_taxonomies where guid = 'd97f38d1-fc28-4168-bf7a-fdd85afa34e3' or guid = 'ac620ec5-9e86-4746-9a61-963356f4e933';

SQL_BREAK;

delete from sandbox.kb_taxonomies where guid in (
select guid 
from sandbox.kb_taxonomies_hier kbh 
where l1_guid = 'c60d65f4-6015-4ed8-b847-d02fd70b5f14' or 
l2_guid = 'c60d65f4-6015-4ed8-b847-d02fd70b5f14' or 
l3_guid = 'c60d65f4-6015-4ed8-b847-d02fd70b5f14' or 
l4_guid = 'c60d65f4-6015-4ed8-b847-d02fd70b5f14' or
l5_guid = 'c60d65f4-6015-4ed8-b847-d02fd70b5f14' or
l6_guid = 'c60d65f4-6015-4ed8-b847-d02fd70b5f14');

SQL_BREAK;

delete from  sandbox.kb_taxonomies where guid in (
select guid 
from sandbox.kb_taxonomies_hier kbh 
where guid = 'b8c12350-fe6b-469f-8e4f-437c8a37aa0d' or
l1_guid = 'b8c12350-fe6b-469f-8e4f-437c8a37aa0d' or 
l2_guid = 'b8c12350-fe6b-469f-8e4f-437c8a37aa0d' or 
l3_guid = 'b8c12350-fe6b-469f-8e4f-437c8a37aa0d' or 
l4_guid = 'b8c12350-fe6b-469f-8e4f-437c8a37aa0d' or
l5_guid = 'b8c12350-fe6b-469f-8e4f-437c8a37aa0d' or
l6_guid = 'b8c12350-fe6b-469f-8e4f-437c8a37aa0d');

SQL_BREAK;

delete from  sandbox.kb_taxonomies where guid in (
select guid 
from sandbox.kb_taxonomies_hier kbh 
where guid = 'db2cb956-fc1a-4d8c-88f2-66657ac41c24' or
l1_guid = 'db2cb956-fc1a-4d8c-88f2-66657ac41c24' or 
l2_guid = 'db2cb956-fc1a-4d8c-88f2-66657ac41c24' or 
l3_guid = 'db2cb956-fc1a-4d8c-88f2-66657ac41c24' or 
l4_guid = 'db2cb956-fc1a-4d8c-88f2-66657ac41c24' or
l5_guid = 'db2cb956-fc1a-4d8c-88f2-66657ac41c24' or
l6_guid = 'db2cb956-fc1a-4d8c-88f2-66657ac41c24');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','294ea30b-dd37-49a1-9805-e9e6c7617902','Beauty');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','294ea30b-dd37-49a1-9805-e9e6c7617903','Wellness');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','9308a9a1-d736-42f7-b66b-f301f1fd652f','Beauty');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','9308a9a1-d736-42f7-b66b-f301f1fd652f','Gesundheit');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','1d73e2c7-995a-4e27-a424-734e366888f7','Gesundheit');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','1d73e2c7-995a-4e27-a424-734e366888f7','Fitness');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','343b6bbd-a979-4401-b82b-c793bfb4dd6e','Computer');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','343b6bbd-a979-4401-b82b-c793bfb4dd6e','Tablets');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','f66acf4e-ed76-4d0b-ae71-0449a7cd3457','Haus');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','f66acf4e-ed76-4d0b-ae71-0449a7cd3457','Garten');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','938de4d2-b8f5-41c7-860f-94ded05f43c8','Freizeit');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','938de4d2-b8f5-41c7-860f-94ded05f43c8','Events');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','7fef3478-e7da-4fbe-b015-e6d240558e5e','Tickets');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','7fef3478-e7da-4fbe-b015-e6d240558e5e','Events');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','555fda07-8c78-4447-a583-fec8133169c6','Maniküre');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','555fda07-8c78-4447-a583-fec8133169c6','Pediküre');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','f052f491-36c2-406f-a196-be2c59d281f4','Essen');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','f052f491-36c2-406f-a196-be2c59d281f4','Trinken');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','a3bd4cfd-b0e1-4a6b-b256-b8bb3927ecb9','Gesundheit');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','a3bd4cfd-b0e1-4a6b-b256-b8bb3927ecb9','Wellness');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','c4788e6b-2a57-4a51-bc86-b2cf4f78108a','Theater');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','c4788e6b-2a57-4a51-bc86-b2cf4f78108a','Shows');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','ea15e199-38b5-4012-a599-6a722d059c80','Lebensmittel');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('de_DE','ea15e199-38b5-4012-a599-6a722d059c80','Süßigkeiten');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','d940e067-f90a-46a9-9c21-8eade45ab971','Épicerie');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','d940e067-f90a-46a9-9c21-8eade45ab971','Marchés');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','4de0bea0-9df2-456f-aabf-3d6fc081dfe6','Alimentation');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','4de0bea0-9df2-456f-aabf-3d6fc081dfe6','Boissons');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','f052f491-36c2-406f-a196-be2c59d281f4','Bars');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','f052f491-36c2-406f-a196-be2c59d281f4','Restaurants');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','294ea30b-dd37-49a1-9805-e9e6c7617902','Beauté');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','294ea30b-dd37-49a1-9805-e9e6c7617902','Bien-être');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','ebe66b35-1f90-41bd-85c3-6afc632ed4a9','Bijoux');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','ebe66b35-1f90-41bd-85c3-6afc632ed4a9','Montres');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','432db251-f8c0-4292-a4d7-ec09f59b3477','Café');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','432db251-f8c0-4292-a4d7-ec09f59b3477','Gourmandises');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','1d73e2c7-995a-4e27-a424-734e366888f7','Forme');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','b8eaf3c9-7bed-4167-baba-e563781e89f9','Santé');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','1d73e2c7-995a-4e27-a424-734e366888f7','Forme');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','b8eaf3c9-7bed-4167-baba-e563781e89f9','Santé');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','938de4d2-b8f5-41c7-860f-94ded05f43c8','Loisirs');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','938de4d2-b8f5-41c7-860f-94ded05f43c8','Sorties');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','f66acf4e-ed76-4d0b-ae71-0449a7cd3457','Maison');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','f66acf4e-ed76-4d0b-ae71-0449a7cd3457','Jardin');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','f1ed1965-b3ad-4b08-a6b5-fd12714d5eb9','Santé');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','9308a9a1-d736-42f7-b66b-f301f1fd652f','Beauté');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','f1ed1965-b3ad-4b08-a6b5-fd12714d5eb9','Santé');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','9308a9a1-d736-42f7-b66b-f301f1fd652f','Beauté');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','a0e884fd-3ace-4fd6-ae8e-80199d0c40b8','Sports');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('fr_FR','a0e884fd-3ace-4fd6-ae8e-80199d0c40b8','Loisirs');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','1d73e2c7-995a-4e27-a424-734e366888f7','Salute ');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','1d73e2c7-995a-4e27-a424-734e366888f7','Sport');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','413b6bbf-9671-40bd-8dc0-3a44342c6acf','Giardini');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','413b6bbf-9671-40bd-8dc0-3a44342c6acf','Orti botanici');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','69b3c3a7-6cb8-4b6f-8002-7b8aeb95b599','Corsi');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','69b3c3a7-6cb8-4b6f-8002-7b8aeb95b599','Servizi');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','a3bd4cfd-b0e1-4a6b-b256-b8bb3927ecb9','Benessere');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','a3bd4cfd-b0e1-4a6b-b256-b8bb3927ecb9','Salute');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','938de4d2-b8f5-41c7-860f-94ded05f43c8','Eventi');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','938de4d2-b8f5-41c7-860f-94ded05f43c8','Tempo libero');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','3e2fd955-3270-44b4-9122-bd3fca371cbc','Auto');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','3e2fd955-3270-44b4-9122-bd3fca371cbc','Furgoni');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','555fda07-8c78-4447-a583-fec8133169c6','Manicure');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','555fda07-8c78-4447-a583-fec8133169c6','Pedicure');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','97e05099-2fb1-4441-99a7-bace36ec38c6','Solidarietà');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('it_IT','97e05099-2fb1-4441-99a7-bace36ec38c6','Volontariato');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','134645a7-73b0-44da-aa7c-68e4d03931a9','Arte');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','134645a7-73b0-44da-aa7c-68e4d03931a9','Decoraciòn');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','bc99dc37-4bcd-4799-9b41-d42c726a8681','Teatro');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','bc99dc37-4bcd-4799-9b41-d42c726a8681','Espectaculos');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','f1ed1965-b3ad-4b08-a6b5-fd12714d5eb9','Salud');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','f1ed1965-b3ad-4b08-a6b5-fd12714d5eb9','Belleza');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','d0ec344a-b07e-453c-8a00-736ae5b9a581','Cerveza');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','d0ec344a-b07e-453c-8a00-736ae5b9a581','Vino');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','d0ec344a-b07e-453c-8a00-736ae5b9a581','Licores');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','a3bd4cfd-b0e1-4a6b-b256-b8bb3927ecb9','Salud');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','a3bd4cfd-b0e1-4a6b-b256-b8bb3927ecb9','Bienestar');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','d940e067-f90a-46a9-9c21-8eade45ab971','Supermercado');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','d940e067-f90a-46a9-9c21-8eade45ab971','Mercados');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','ebe66b35-1f90-41bd-85c3-6afc632ed4a9','Joyerìa');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','ebe66b35-1f90-41bd-85c3-6afc632ed4a9','Relojes');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','3ae5acf1-55a4-4c2e-95a9-4479ac70e0ae','Salud para bebes');

SQL_BREAK;

insert into sandbox.kb_taxonomy_updates values('es_ES','3ae5acf1-55a4-4c2e-95a9-4479ac70e0ae','Seguridad para bebes');

SQL_BREAK;

replace view sandbox.kb_taxonomies_view as
select T.* from sandbox.kb_taxonomies T
union 
select TT.locale, TT.category_name, 
       TT.description, TT.guid, 
       TT.taxonomy_guid, TT.child_count, 
       TT.parent, TT.depth, 
       TT.friendly_namePlural, TT.permalink, 
       TT.seo_name, TT.friendly_name_short, TT.friendly_name, 
       TT.friendly_name_singular, 
       U.friendly_name_title,  
       TT.relationships_size
from sandbox.kb_taxonomy_updates U
left join sandbox.kb_taxonomies TT on TT.locale = U.locale and TT.guid = U.guid;
