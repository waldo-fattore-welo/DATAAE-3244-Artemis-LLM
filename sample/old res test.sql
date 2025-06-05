-- Annotation Result Data
with res as
    (
    select r.id::TEXT result_id
        ,a.id annotation_id
        ,t.id task_id
        ,t.project_id
        ,json_extract_path_text(p.description,'project_type') project_type
        ,a.completed_by.id rater_id

         ,r.origin::TEXT origin
        ,r.to_name::TEXT to_name
        ,r.from_name::TEXT from_name
        ,r.value.choices[0]::TEXT choices

    from labelstudio.tasks t
       join labelstudio.projects p on p.id = t.project_id
       ,t.annotations a
       ,a.result r

    where t.project_id in (select p.id
        from labelstudio.projects p
        where p.workspace in (80322) --Artemis - LLM Training
            and p.id in (143483, 143484) --(Batch Pilot - wk1 v2) US - Blinds/Arbitration
    )
    and (t.id = 173577729 or t.data.part_1_task_id::INT = 173577729) --matching pair 2+1
)
,
    roles as (
    select a.id annotation_id
        ,json_extract_path_text(p.description,'project_type') project_type
        ,t.id task_id
        ,ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY annotation_id) as rater_num
        ,case when project_type = 'Annotation' then concat('R', rater_num) else 'ARB' end rater_role
    --     ,t.data.annotator_1_id::INT ann1
    --     ,t.data.annotator_2_id::INT ann2

    from labelstudio.tasks t
        join labelstudio.projects p on p.id = t.project_id
        ,t.annotations a
    where t.project_id in (
        select p.id
        from labelstudio.projects p
        where p.workspace in (80322) --Artemis - LLM Training
            and p.id in (143483, 143484) --(Batch Pilot - wk1 v2) US - Blinds/Arbitration
        )
        and (t.id = 173577729 or t.data.part_1_task_id::INT = 173577729) --matching pair 2+1
)

select res.project_id
    ,res.task_id
    ,res.annotation_id
    ,res.result_id
    ,res.project_type

    ,res.rater_id
    ,roles.rater_role

    ,res.origin
    ,res.to_name
    ,res.from_name
    ,res.choices

from res
    join roles on roles.annotation_id = res.annotation_id
where res.from_name in ('q1','q3','q4','q6','q7','qa','qb','qc')
--QA = q1 q2
--QB = q3 q4 q5
--QC = q6 q7 q8