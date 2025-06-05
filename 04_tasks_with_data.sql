-- Tasks w/Data
select t.id task_id
    ,t.project_id
    ,t.data.part_1_task_id::INT
    ,t.data.welo_unique_id::TEXT

    ,t.created_at
    ,t.completed_at
    ,t.updated_at
    --,t.annotations
    ,t.comments

    ,t.data.annotator_1_id::INT
    ,t.data.annotator_2_id::INT

    ,t.data.pt::TEXT
    ,t.data.org::TEXT
    ,t.data.link::TEXT
    ,t.data.week::TEXT
    ,t.data.row_id::TEXT
    ,t.data.pickers::TEXT
    ,t.data.keywords::TEXT
    ,t.data.platform::TEXT
    ,t.data.subbatch::TEXT
    ,t.data.refinement::TEXT
    ,t.data.multipt_pickers
    ,t.data.pickers_choices
    ,t.data.refinement_choices

    ,t.data.claude_reason::TEXT
    ,t.data.claude_relevant::TEXT
    ,t.data.claude_relevant_rib::TEXT
    ,t.data.claude_preference_aware::TEXT
    ,t.data.claude_relevant_defects::TEXT
    ,t.data.claude_irrelevant_pickers::TEXT
    ,t.data.claude_preference_aware_rib::TEXT
    ,t.data.claude_relevant_rib_defects::TEXT
    ,t.data.claude_irrelevant_refinement::TEXT
    ,t.data.claude_preference_aware_defects::TEXT
    ,t.data.claude_preference_aware_rib_defects::TEXT
    ,t.data.claude_preference_violating_pickers::TEXT
    ,t.data.claude_preference_violating_refinement::TEXT

from labelstudio.tasks t
where t.project_id in (
    select p.id
    from labelstudio.projects p
    where p.workspace in (80322) --Artemis - LLM Training
        -- and p.id in (143483, 143484) --(Batch Pilot - wk1 v2) US - Blinds/Arbitration
    )
    -- and (t.id = 173577729 or t.data.part_1_task_id::INT = 173577729) --matching pair 2+1
; 