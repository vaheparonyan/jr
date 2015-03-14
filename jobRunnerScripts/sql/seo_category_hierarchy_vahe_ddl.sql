create table sandbox.seo_category_hierarchy_vahe as
 ( select p.seo_id as seo_id,    
   case when c.parent1_name is null then  
     case when (c.name = 'ALL') or (c.name is null) then p.name  
       else c.name end 
     else c.parent1_name end as top_level_category,   
   case when c.parent1_name is null then p.name else c.name end as mid_level_category,    
   p.name as detail_category  
  from ( select name, parent1, seo_id from sandbox.seo_categories_with_names ) p  
  left join sandbox.seo_categories_with_names c  
on p.parent1 = c.seo_id ) with data primary index ( seo_id ) ;
 


