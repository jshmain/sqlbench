
select  s_store_name
      ,sum(ss_net_profit)
 from store_sales
     ,date_dim
     ,store,
     (select ca_zip
     from (
      SELECT substr(ca_zip,1,5) ca_zip
      FROM customer_address
      WHERE substr(ca_zip,1,5) IN (
                          '37845','65250','69987','50150','38931','46042',
                          '52877','40879','92244','63566','38066',
                          '78166','27884','93043','67988','22504',
                          '80448','71730','68841','92613','55696',
                          '50044','29557','54181','70666','10934',
                          '29821','20279','55481','64141','32691',
                          '77827','29904','59268','63811','26059',
                          '11439','69150','68770','38822','38613',
                          '79601','41394','79337','74924','24139',
                          '59716','51794','42119','62135','70076',
                          '89637','54433','71271','79291','32336',
                          '78125','13993','59531','20904','87723',
                          '37741','28550','25354','72764','76854',
                          '58160','97409','58229','24156','76872',
                          '73991','13944','41348','42094','19815',
                          '32341','14537','49426','24065','97593',
                          '40095','20202','26502','20679','47166',
                          '19644','10405','42208','24299','94359',
                          '50247','17073','62570','49746','91567',
                          '58186','30338','24439','27734','61502',
                          '97053','40343','15829','68811','69795',
                          '23178','34617','99947','17682','46929',
                          '96232','72405','42949','53636','18379',
                          '99057','51195','78467','81568','57588',
                          '91945','61595','37565','97943','95969',
                          '19114','50180','41007','46922','58679',
                          '78591','73746','50347','12069','53822',
                          '35696','34713','83672','29902','42674',
                          '94832','24739','93069','90873','59791',
                          '95641','17562','32310','47007','27973',
                          '97134','61955','45320','63455','27601',
                          '52290','56761','12517','88983','76124',
                          '67796','76601','91035','15239','60923',
                          '26652','31383','44019','24720','35227',
                          '47680','63796','11307','67845','60954',
                          '21475','59586','41650','14948','97386',
                          '79353','59492','99651','83796','46410',
                          '73497','82267','30624','92421','45820',
                          '61969','95186','40513','14579','33016',
                          '15948','23921','18440','14153','70753',
                          '37296','28819','47282','51835','13001',
                          '65063','25553','12670','22058','94223',
                          '70548','89450','94966','11634','90212',
                          '23412','81254','12762','14305','57336',
                          '58223','37996','62777','43840','33746',
                          '90085','97449','60557','24812','14921',
                          '46880','37440','16463','78931','19790',
                          '10965','76547','19334','32770','28040',
                          '57150','14744','59269','10964','67134',
                          '13432','81201','23098','26885','27845',
                          '97639','12492','68891','88652','34850',
                          '15676','14237','67324','42578','37865',
                          '10737','43718','40181','39956','30942',
                          '46645','43843','26738','74663','14985',
                          '50817','29516','74257','81565','40598',
                          '16266','64825','40211','55151','21155',
                          '11413','13963','92798','71115','80371',
                          '36626','17066','78242','64892','21688',
                          '65130','18554','82719','21518','18224',
                          '61378','95323','83810','10416','57945',
                          '87225','74511','43254','75401','45504',
                          '58533','40286','73017','39932','45507',
                          '45245','85558','96577','30274','98267',
                          '41659','57553','69427','64143','34820',
                          '18354','75773','30592','14611','34270',
                          '56634','18048','96032','48589','30590',
                          '94874','32038','41091','39359','11201',
                          '27255','15130','52784','56822','45481',
                          '67731','49854','10389','27108','71239',
                          '70001','68462','82516','44858','43464',
                          '62401','19594','21361','11510','58419',
                          '87435','82586','26903','71016','46564',
                          '76103','90918','27354','50945','27030',
                          '67748','25270','73868','34555','14474',
                          '72403','27806','13688','46322','59197',
                          '22961','30312','26235','55133','90974',
                          '49359','46898','20018','86694','55232',
                          '68481','90204','96544','55713','92534',
                          '38176','57004','38677','27460','43935',
                          '98669','65546','91248','36465')
     intersect
      select ca_zip
      from (SELECT substr(ca_zip,1,5) ca_zip,count(*) cnt
            FROM customer_address, customer
            WHERE ca_address_sk = c_current_addr_sk and
                  c_preferred_cust_flag='Y'
            group by ca_zip
            having count(*) > 10)A1)A2) V1
 where ss_store_sk = s_store_sk
  and ss_sold_date_sk = d_date_sk
  and d_qoy = 1 and d_year = 2000
  and (substr(s_zip,1,2) = substr(V1.ca_zip,1,2))
 group by s_store_name
 order by s_store_name
 limit 100;

