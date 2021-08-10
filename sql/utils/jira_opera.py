from django.db import transaction
from django.utils import timezone
from django.shortcuts import render
from jira import JIRA, exceptions
from common.config import SysConfig
from sql.models import ResourceGroup
from sql.models import SqlWorkflowContent
from sql.models import SqlWorkflow
from sql.models import Instance
from sql.models import Users
from sql.engines import get_engine
from common.utils.const import WorkflowDict
from sql.utils.workflow_audit import Audit
from sql.utils.resource_group import user_instances


def jira_opera(user):
    jira_url = 'http://jira2.rootcloud.com:8080/'
    # 用户名和密码换成环境变量
    obj = JIRA(basic_auth=("zhipeng.su", "su@12345"), options={'server': jira_url})
    issue = obj.issue('GYPT-55246')
    # reporter = issue.fields.reporter.name
    # 测试用户,因本地环境没有接入ldap校验
    reporter = "admin"
    sql_content = issue.fields.description.strip()
    workflow_title = issue.fields.summary
    demand_url = jira_url + '/browse' + 'GYPT-55246'
    # 检查用户是否有权限涉及到资源组等， 比较复杂， 可以把检查权限改成一个独立的方法
    group_name = '瓜头瓜脑'
    group_id = ResourceGroup.objects.get(group_name=group_name).group_id
    instance_name = 'node2'
    instance = Instance.objects.get(instance_name=instance_name)
    db_name = 'CMDB'
    # is_backup = True if request.POST.get('is_backup') == 'True' else False
    is_backup = False
    # cc_users = request.POST.getlist('cc_users')
    # run_date_start = request.POST.get('run_date_start')
    # run_date_end = request.POST.get('run_date_end')
    run_date_start = None
    run_date_end = None
    res = SqlWorkflow.objects.filter(demand_url=demand_url)

    # 工单参数验证
    if None in [sql_content, instance_name, db_name, is_backup, demand_url]:
        obj.add_comment(issue, "请补全相关参数")
        return "参数不全"

    # 验证组权限（用户是否在该组、该组是否有指定实例）
    try:
        reporter = Users.objects.get(username=reporter)
        user_instances(reporter, tag_codes=['can_write']).get(instance_name=instance_name)
    except instance.DoesNotExist:
        obj.add_comment(issue, "你所在组未关联该实例")
        return "你所在组未关联该实例"

    # 再次交给engine进行检测，防止绕过
    try:
        check_engine = get_engine(instance=instance)
        check_result = check_engine.execute_check(db_name=db_name, sql=sql_content.strip())
    except Exception as e:
        # context = {'errMsg': str(e)}
        obj.add_comment(issue, f"errMsg: {str(e)}")
        # return render(request, 'error.html', context)
        return "检测失败"

    # 按照系统配置确定是自动驳回还是放行
    sys_config = SysConfig()
    auto_review_wrong = sys_config.get('auto_review_wrong', '')  # 1表示出现警告就驳回，2和空表示出现错误才驳回
    workflow_status = 'workflow_manreviewing'
    if check_result.warning_count > 0 and auto_review_wrong == '1':
        workflow_status = 'workflow_autoreviewwrong'
    elif check_result.error_count > 0 and auto_review_wrong in ('', '1', '2'):
        workflow_status = 'workflow_autoreviewwrong'

    # 检测该单是否已经存在
    if res.count() == 0:
        with transaction.atomic():
            # jira工单存进数据库里
            sql_workflow = SqlWorkflow.objects.create(
                workflow_name=workflow_title,
                demand_url=demand_url,
                group_id=group_id,
                group_name=group_name,
                # engineer=request.user.username,
                engineer=reporter,
                # engineer_display=request.user.display,
                audit_auth_groups=Audit.settings(group_id, WorkflowDict.workflow_type['sqlreview']),
                # audit_auth_groups=1,
                status=workflow_status,
                is_backup=is_backup,
                instance=instance,
                db_name=db_name,
                is_manual=0,
                # syntax_type=check_result.syntax_type,
                syntax_type=2,
                create_time=timezone.now(),
                run_date_start=run_date_start or None,
                run_date_end=run_date_end or None
            )
            SqlWorkflowContent.objects.create(workflow=sql_workflow,
                                              sql_content=sql_content,
                                              review_content=check_result.json(),
                                              execute_result=''
                                              )
            workflow_id = sql_workflow.id
            # 自动审核通过了，才调用工作流
            if workflow_status == 'workflow_manreviewing':
                # 调用工作流插入审核信息, 查询权限申请workflow_type=2
                Audit.add(WorkflowDict.workflow_type['sqlreview'], workflow_id)

        obj.add_comment(issue, "工单提交成功")
        return 'Success'
    else:
        return None

