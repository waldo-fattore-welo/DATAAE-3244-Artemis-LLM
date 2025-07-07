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
    ,t.data.keywords::TEXT keywords
    ,t.data.pt_note_top::TEXT pt_note_top
    ,t.data.pt_note_bottom::TEXT pt_note_bottom
    ,t.data.welo_unique_id::TEXT welo_unique_id
    ,t.data.introduction_html::TEXT introduction_html
    ,t.data.refinement_pickers::TEXT refinement_pickers
    ,t.data.claude_relevant_note::TEXT claude_relevant_note
    ,t.data.claude_preference_aware_note::TEXT claude_preference_aware_note

    ,t.data.week
    ,t.data.row_id
    ,t.data.pickers
    ,t.data.platform
    ,t.data.subbatch
    ,t.data.refinement
    ,t.data.rater_1_q1a
    ,t.data.rater_1_q1b
    ,t.data.rater_1_q2a
    ,t.data.rater_1_q2b
    ,t.data.rater_1_q3a
    ,t.data.rater_2_q1a
    ,t.data.rater_2_q1b
    ,t.data.rater_2_q2a
    ,t.data.rater_2_q2b
    ,t.data.rater_2_q3a
    ,t.data.claude_reason
    ,t.data.annotator_1_id
    ,t.data.annotator_2_id
    ,t.data.claude_relevant
    ,t.data.multipt_pickers
    ,t.data.refinement_pickers
    ,t.data.claude_relevant_rib
    ,t.data.claude_preference_aware
    ,t.data.claude_relevant_defects
    ,t.data.claude_preference_aware_rib
    ,t.data.claude_relevant_rib_defects
    ,t.data.claude_preference_aware_defects
    ,t.data.claude_preference_aware_rib_defects

from labelstudio.tasks t
where t.project_id in (
    select p.id
    from labelstudio.projects p
    where p.workspace in (80322) --Artemis - LLM Training
        and p.id in (161719, 161720) --LLM (June25 - Batch 1 v2) en-US - Blinds/Arbitration
    )
    --and (t.id = 190390657 or t.data.part_1_task_id::INT = 189091813) --matching pair 2+1
    --and welo_unique_id = 'd37e1935-86b0-4dae-b474-d91566484a26' --matching pair 2+1
--order by welo_unique_id
; 