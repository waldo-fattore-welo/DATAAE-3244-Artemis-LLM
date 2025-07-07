-- Annotation Result Data
with ann_role as (
    select t.project_id
        ,t.id task_id
        ,a.id annotation_id
        ,t.data.welo_unique_id::TEXT welo_unique_id

        ,json_extract_path_text(p.description,'project_type') project_type
        ,a.completed_by.id rater_id
        ,ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY annotation_id) as rater_num
        ,case when project_type = 'Annotation' then concat('R', rater_num) else 'ARB' end rater_role
        ,a.result result
    from labelstudio.tasks t
       join labelstudio.projects p on p.id = t.project_id
       ,t.annotations a

    where t.project_id in (select p.id
        from labelstudio.projects p
        where p.workspace in (80322) --Artemis - LLM Training
            and p.id in (161719, 161720) --LLM (June25 - Batch 1 v2) en-US - Blinds/Arbitration
    )
    --and (t.id = 173577729 or t.data.part_1_task_id::INT = 173577729) --matching pair 2+1
    and welo_unique_id = 'd37e1935-86b0-4dae-b474-d91566484a26' --matching pair 2+1
)
,
res as (
select ar.project_id
    ,ar.task_id
    ,ar.annotation_id
    ,ar.welo_unique_id
    ,re.id::TEXT result_id

    ,ar.project_type
    ,ar.rater_id
    ,ar.rater_role

    --,re.origin::TEXT origin
    ,re.to_name::TEXT to_name
    ,REPLACE(re.from_name::TEXT,'_best_answer','') from_name
    ,re.from_name::TEXT from_name_original
    ,re.value.choices[0]::TEXT choices

from ann_role ar
    ,ar.result re
)
--select * from res order by rater_role, from_name

,
piv_roles as (
select
    res.welo_unique_id
    ,res.from_name

    ,MAX(case when res.rater_role = 'R1' then res.rater_id end) R1_id
    ,MAX(case when res.rater_role = 'R2' then res.rater_id end) R2_id
    ,MAX(case when res.rater_role = 'ARB' then res.rater_id end) ARB_id

    ,MAX(case when res.rater_role = 'R1' then res.choices end) R1_choice
    ,MAX(case when res.rater_role = 'R2' then res.choices end) R2_choice
    ,MAX(case when res.rater_role = 'ARB' then res.choices end) ARB_choice
from res
-- where res.from_name in ('q1','q3','q4','q6','q7')
--     and res.project_type = 'Annotation'
group by res.welo_unique_id
    ,res.from_name
)
--select * from piv_roles

select piv_roles.welo_unique_id
    ,piv_roles.from_name
    ,piv_roles.R1_id
    ,piv_roles.R2_id
    ,piv_roles.ARB_id
    ,piv_roles.R1_choice
    ,piv_roles.R2_choice
    ,piv_roles.ARB_choice
    ,case when (R1_choice = R2_choice) and (ARB_choice!='Neither' or ARB_choice is null) then 1 else 0 end agree_ALL
    ,case when R1_choice = R2_choice then 1 else 0 end agree_R1_R2
    ,case when ARB_choice is null then null
        when agree_ALL=1 or ARB_choice in  ('Rater 1','Both') then 1 else 0 end agree_R1_ARB
    ,case when ARB_choice is null then null
        when agree_ALL=1 or ARB_choice in  ('Rater 2','Both') then 1 else 0 end agree_R2_ARB

from piv_roles;