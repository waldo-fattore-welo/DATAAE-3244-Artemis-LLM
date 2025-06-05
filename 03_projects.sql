-- Projects
select p.id project_id
    ,json_extract_path_text(p.description,'source_language') source_language
    ,json_extract_path_text(p.description,'target_language') target_language
    ,json_extract_path_text(p.description,'batch') batch
    ,json_extract_path_text(p.description,'project_type') project_type
    ,p.finished_task_number
    ,p.is_published
    ,p.model_version
    ,p.ready
    ,p.task_number
    ,p.title
    ,p.workspace
    --,p.m_completed
from labelstudio.projects p
where p.workspace in (80322) --Artemis - LLM Training
; 