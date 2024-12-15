用户个数：

SELECT COUNT(distinct x.user_id) FROM public.ratings x; 1424595
SELECT COUNT(distinct x.user_id) FROM public.ratings x where x.doc_embedding notnull; 471349

with t as 
(SELECT x.user_id as uid, count(1) as cnt FROM public.ratings x where x.doc_embedding notnull group by x.user_id)
select * from t; ：用户的评论数分布见 .csv 附件。

