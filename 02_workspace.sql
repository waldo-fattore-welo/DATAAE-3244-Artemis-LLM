-- Identify Workspace
select w.id workspace_id
    ,w.title workspace_name
from labelstudio.workspaces w
where w.id = 80322
; 