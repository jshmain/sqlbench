
select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 76 and 76+10 
             or ss_coupon_amt between 4024 and 4024+1000
             or ss_wholesale_cost between 66 and 66+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 113 and 113+10
          or ss_coupon_amt between 15626 and 15626+1000
          or ss_wholesale_cost between 59 and 59+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 88 and 88+10
          or ss_coupon_amt between 1234 and 1234+1000
          or ss_wholesale_cost between 67 and 67+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 34 and 34+10
          or ss_coupon_amt between 3772 and 3772+1000
          or ss_wholesale_cost between 30 and 30+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 11 and 11+10
          or ss_coupon_amt between 8082 and 8082+1000
          or ss_wholesale_cost between 28 and 28+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 50 and 50+10
          or ss_coupon_amt between 15688 and 15688+1000
          or ss_wholesale_cost between 11 and 11+20)) B6
limit 100;


