-- Annotation Data
select a.id annotation_id
    ,t.id task_id
    ,t.project_id
    ,t.data.welo_unique_id::TEXT
    ,json_extract_path_text(p.description,'project_type') project_type

    ,a.completed_by.id rater_id
    ,ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY annotation_id) as rater_num
    ,case when project_type = 'Annotation' then concat('R', rater_num) else 'ARB' end rater_role
--     ,t.data.annotator_1_id::INT ann1
--     ,t.data.annotator_2_id::INT ann2
    ,a.lead_time

    ,a.last_annotation_history
    ,a.was_cancelled
    ,a.ground_truth
    ,a.created_at::TIMESTAMP
    ,a.updated_at::TIMESTAMP
    ,a.draft_created_at::TIMESTAMP

    ,a.import_id
    ,a.last_action::TEXT
    ,a.bulk_created
    ,a.updated_by
    ,a.parent_prediction
    ,a.parent_annotation
    ,a.last_created_by
    ,a.comment_count
    ,a.unresolved_comment_count

from labelstudio.tasks t
    join labelstudio.projects p on p.id = t.project_id
    ,t.annotations a
where t.project_id in (
    select p.id
    from labelstudio.projects p
    where p.workspace in (80322) --Artemis - LLM Training
        and p.id in (161719, 161720) --LLM (June25 - Batch 1 v2) en-US - Blinds/Arbitration
    )
    --and (t.id = 173577729 or t.data.part_1_task_id::INT = 173577729) --matching pair 2+1
    --and welo_unique_id = 'd37e1935-86b0-4dae-b474-d91566484a26' --matching pair 2+1
; 