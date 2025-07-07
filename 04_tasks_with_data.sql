-- Tasks w/Data
select t.id task_id
    ,t.project_id
    ,t.data.part_1_task_id::INT part_1_task_id
    ,t.data.welo_unique_id::TEXT welo_unique_id

    ,t.created_at
    ,t.completed_at
    ,t.updated_at
    --,t.annotations
    ,t.comments

    ,t.data.annotator_1_id::INT annotator_1_id
    ,t.data.annotator_2_id::INT annotator_2_id

    ,t.data.pt::TEXT pt
    ,t.data.org::TEXT org
    ,t.data.link::TEXT link
    ,t.data.week::TEXT week
    ,t.data.row_id::TEXT row_id
    ,t.data.pickers::TEXT pickers
    ,t.data.keywords::TEXT keywords
    ,t.data.platform::TEXT platform
    ,t.data.subbatch::TEXT subbatch
    ,t.data.refinement::TEXT refinement
    ,t.data.multipt_pickers multipt_pickers
    ,t.data.pickers_choices pickers_choices
    ,t.data.refinement_choices refinement_choices

    ,t.data.claude_reason::TEXT claude_reason
    ,t.data.claude_relevant::TEXT claude_relevant
    ,t.data.claude_relevant_rib::TEXT claude_relevant_rib
    ,t.data.claude_preference_aware::TEXT claude_preference_aware
    ,t.data.claude_relevant_defects::TEXT claude_relevant_defects
    ,t.data.claude_irrelevant_pickers::TEXT claude_irrelevant_pickers
    ,t.data.claude_preference_aware_rib::TEXT claude_preference_aware_rib
    ,t.data.claude_relevant_rib_defects::TEXT claude_relevant_rib_defects
    ,t.data.claude_irrelevant_refinement::TEXT claude_irrelevant_refinement
    ,t.data.claude_preference_aware_defects::TEXT claude_preference_aware_defects
    ,t.data.claude_preference_aware_rib_defects::TEXT claude_preference_aware_rib_defects
    ,t.data.claude_preference_violating_pickers::TEXT claude_preference_violating_pickers
    ,t.data.claude_preference_violating_refinement::TEXT claude_preference_violating_refinement

from labelstudio.tasks t
where t.project_id in (
    select p.id
    from labelstudio.projects p
    where p.workspace in (80322) --Artemis - LLM Training
        --and p.id in (161719, 161728) --LLM (June25 - Batch 1 v2) en-US - Blinds/Arbitration
    )
    --and (t.id = 190390657 or t.data.part_1_task_id::INT = 189091813) --matching pair 2+1
    --and welo_unique_id = '0696eba7-ce3c-4cfd-8700-cf9277fefe5e' --matching pair 2+1
--order by welo_unique_id
; 