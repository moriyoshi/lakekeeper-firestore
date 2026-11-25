//! The `CatalogStore` implementation. Methods not yet backed by Firestore answer
//! "unsupported" (see [`unsupported`]).

use std::collections::{HashMap, HashSet};

use chrono::Duration;
use lakekeeper::{
    SecretId,
    api::ErrorModel,
    api::{
        iceberg::v1::{
            PaginatedMapping, PaginationQuery, namespace::NamespaceDropFlags,
            tables::LoadTableFilters,
        },
        management::v1::{
            DeleteWarehouseQuery, TabularType,
            project::{EndpointStatisticsResponse, TimeWindowSelector, WarehouseFilter},
            role::UpdateRoleSourceSystemRequest,
            task_queue::{GetTaskQueueConfigResponse, SetTaskQueueConfigRequest},
            tasks::ListTasksRequest,
            user::{ListUsersResponse, SearchUserResponse, UserLastUpdatedWith, UserType},
            warehouse::{TabularDeleteProfile, WarehouseStatisticsResponse},
        },
    },
    iceberg::{NamespaceIdent, spec::ViewMetadata},
    service::{
        AddRoleMembersError, AddRoleMembersResult, AddUserRoleAssignmentsError,
        AddUserRoleAssignmentsResult, ApplyGrantsStoreError, ApplyTagError, ArcProjectId,
        AssignedRole, CatalogBackendError, CatalogCreateNamespaceError, CatalogCreateRoleRequest,
        CatalogCreateTagDefinitionRequest, CatalogCreateWarehouseError,
        CatalogCreateWarehouseRequest, CatalogDeleteWarehouseError, CatalogGetNamespaceError,
        CatalogGetWarehouseByIdError, CatalogGetWarehouseByNameError, CatalogListNamespaceError,
        CatalogListNamespacesResponse, CatalogListRolesByIdFilter, CatalogListWarehousesError,
        CatalogMoveNamespaceError, CatalogNamespaceDropError, CatalogRenameWarehouseError,
        CatalogRoleForAssignment, CatalogSearchTabularResponse, CatalogSetNamespaceProtectedError,
        CatalogStore, CatalogUpdateNamespacePropertiesError, CatalogUserRoleAssignmentUser,
        CatalogView, ClearTabularDeletedAtError, CommitTableTransactionError, CommitViewError,
        CreateGenericTableError, CreateNamespaceRequest, CreateOrUpdateUserResponse,
        CreateRoleError, CreateTableError, CreateTagDefinitionError, CreateViewError,
        DeleteRoleError, DeleteTagDefinitionError, DeletedUser, DeletedWarehouse,
        DropGenericTableError, DropTabularError, EffectiveTagCandidate,
        EnsureWarehouseSpecMutableError, GenericTableCreation, GenericTableId, GenericTableInfo,
        GenericTableListEntry, GetProjectResponse, GetTabularInfoByLocationError,
        GetTabularInfoError, GetTaskDetailsError, ListCatalogRoleMembersPage,
        ListGenericTablesError, ListGrantsStoreError, ListNamespacesQuery, ListRoleMembersResult,
        ListRolesError, ListRolesPage, ListRolesResponse, ListTabularsError,
        ListTagAttachmentsError, ListTagAttachmentsResponse, ListTagDefinitionsError,
        ListTagDefinitionsResponse, ListUserRoleAssignmentsResult, LoadGenericTableError,
        LoadTableError, LoadTableResponse, LoadViewError, Location, ManagedBy,
        MarkTabularAsDeletedError, MovedNamespace, NamespaceDropInfo, NamespaceId,
        NamespaceWithParent, OnRoleConflict, ProjectId, ReboundRole, RemoveRoleMembersError,
        RemoveRoleMembersResult, RemoveTagError, RemoveUserRoleAssignmentsError,
        RemoveUserRoleAssignmentsResult, RenameTabularError, RenamedWarehouse, ResolveTasksError,
        ResolvedTask, ResolvedWarehouse, Result, RevokeSubtreeGrantsStoreError, Role, RoleId,
        RoleIdent, RoleMemberKind, RoleMembershipDirection, RoleMembershipEntry, RoleProviderId,
        SearchRoleResponse, SearchRolesError, SearchTabularError, ServerId, ServerInfo,
        SetTabularProtectionError, SetWarehouseDeletionProfileError,
        SetWarehouseFormatVersionPolicyError, SetWarehouseManagedByError,
        SetWarehouseProtectedError, SetWarehouseStatusError, StagedTableId, StateOrTransaction,
        StateOrTransactionEnum, SyncFor, SyncRoleMembersError, SyncRoleMembersResult,
        SyncUserRoleAssignmentsError, SyncUserRoleAssignmentsResult, TableCommit, TableCreation,
        TableId, TableIdent, TableInfo, TabularId, TabularIdentBorrowed, TabularListFlags, Tag,
        TagAttachmentFilter, TagDefinition, TagDefinitionId, TagId, TagSource, TagTarget,
        TagWithName, TaskDetails, TaskList, Transaction, UpdateRoleError, UpdateTagDefinitionError,
        UpdateTagDefinitionRequest, UpdateWarehouseStorageProfileError, UpdatedNamespaceProperties,
        UpdatedWarehouseStorage, UserMembershipEntry, UserUpsertMode, ViewCommit, ViewId, ViewInfo,
        ViewOrTableDeletionInfo, ViewOrTableInfo, WarehouseFormatVersionPolicy, WarehouseId,
        WarehouseStatus,
        authn::UserId,
        authz::{
            AppliedGrants, GrantCandidate, GrantFilter, GrantResource, GrantRevokeCandidates,
            GrantSpec, ListGrantsResultPage, ListSubtreeGrantsResultPage, SubtreeGrantFilter,
            SubtreeGrantRoot, UserOrRoleId,
        },
        cache_invalidation::{BoxedCacheChangeLog, ChangeSet},
        storage::StorageProfile,
        task_configs::TaskQueueConfigFilter,
        tasks::{
            CancelTasksFilter, EnqueueOutcome, MaterializedTick, PickTaskRequest,
            ProjectTaskScheduleState, RetryDecision, Task, TaskAttemptId, TaskCheckState,
            TaskDetailsScope, TaskFilter, TaskId, TaskInput, TaskQueueDemand,
            TaskQueueDemandRequest, TaskQueueName, TaskResolveScope, TaskSchedule,
            TaskScheduleFilter, TaskScheduleId, TaskScheduleStoreError, TaskScheduleUpsert,
            TriggerTaskScheduleError, UpsertTaskScheduleError,
        },
    },
};

use super::{FirestoreBackend, FirestoreState, FirestoreTransaction, unsupported};

#[async_trait::async_trait]
#[allow(unused_variables)]
impl CatalogStore for FirestoreBackend {
    type Transaction = FirestoreTransaction;
    type State = FirestoreState;

    async fn get_server_info(catalog_state: Self::State) -> Result<ServerInfo, ErrorModel> {
        super::server::get_server_info(&catalog_state).await
    }

    async fn bootstrap<'a>(
        terms_accepted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<bool> {
        super::server::bootstrap(terms_accepted, &mut transaction.txn).await
    }

    async fn reopen_for_bootstrap(catalog_state: Self::State) -> Result<ServerId> {
        super::server::reopen_for_bootstrap(&catalog_state).await
    }

    async fn record_cache_changes_impl<'a>(
        change_set: &ChangeSet,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), CatalogBackendError> {
        super::cache_change_log::record(&mut transaction.txn, change_set)
    }

    fn cache_change_log(catalog_state: Self::State) -> BoxedCacheChangeLog {
        Box::new(super::cache_change_log::FirestoreChangeLog::new(
            catalog_state.db.clone(),
        ))
    }

    async fn prune_cache_change_log(
        catalog_state: Self::State,
        retention: std::time::Duration,
    ) -> std::result::Result<Option<u64>, CatalogBackendError> {
        super::cache_change_log::prune(&catalog_state.db, retention)
            .await
            .map(Some)
    }

    async fn create_project<'a>(
        project_id: &ProjectId,
        project_name: String,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        super::project::create_project(project_id, project_name, &mut transaction.txn).await
    }

    async fn delete_project_impl<'a>(
        project_id: &ProjectId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Vec<UserId>> {
        super::project::delete_project(project_id, &mut transaction.txn).await
    }

    async fn get_project<'a>(
        project_id: &ProjectId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<GetProjectResponse>> {
        super::project::get_project(project_id, &mut transaction.txn).await
    }

    async fn list_projects(
        project_ids: Option<HashSet<ProjectId>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<GetProjectResponse>> {
        super::project::list_projects(project_ids, &mut transaction.txn).await
    }

    async fn rename_project<'a>(
        project_id: &ProjectId,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        super::project::rename_project(project_id, new_name, &mut transaction.txn).await
    }

    async fn create_warehouse_impl<'a>(
        project_id: &ProjectId,
        request: CatalogCreateWarehouseRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<ResolvedWarehouse, CatalogCreateWarehouseError> {
        super::warehouse::create_warehouse(project_id, request, &mut transaction.txn).await
    }

    async fn delete_warehouse_impl<'a>(
        warehouse_id: WarehouseId,
        query: DeleteWarehouseQuery,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<DeletedWarehouse, CatalogDeleteWarehouseError> {
        super::warehouse::delete_warehouse(warehouse_id, query, &mut transaction.txn).await
    }

    async fn rename_warehouse_impl<'a>(
        warehouse_id: WarehouseId,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<RenamedWarehouse, CatalogRenameWarehouseError> {
        super::warehouse::rename_warehouse(warehouse_id, new_name, &mut transaction.txn).await
    }

    async fn list_warehouses_impl(
        project_id: &ProjectId,

        status_filter: Option<Vec<WarehouseStatus>>,
        state: Self::State,
    ) -> std::result::Result<Vec<ResolvedWarehouse>, CatalogListWarehousesError> {
        Ok(
            super::warehouse::list_warehouses(project_id, status_filter, &mut state.latest())
                .await?,
        )
    }

    async fn get_warehouse_by_id_impl<'a>(
        warehouse_id: WarehouseId,
        state: Self::State,
    ) -> std::result::Result<Option<ResolvedWarehouse>, CatalogGetWarehouseByIdError> {
        let doc = super::warehouse::read(&mut state.latest(), warehouse_id).await?;
        Ok(doc.map(|d| super::warehouse::resolve(&d)).transpose()?)
    }

    async fn get_warehouse_by_name_impl(
        warehouse_name: &str,
        project_id: &ProjectId,
        catalog_state: Self::State,
    ) -> Result<Option<ResolvedWarehouse>, CatalogGetWarehouseByNameError> {
        Ok(super::warehouse::get_warehouse_by_name(
            warehouse_name,
            project_id,
            &mut catalog_state.latest(),
        )
        .await?)
    }

    async fn get_warehouse_stats(
        warehouse_id: WarehouseId,
        pagination_query: PaginationQuery,
        state: Self::State,
    ) -> Result<WarehouseStatisticsResponse> {
        Ok(super::warehouse::empty_stats(warehouse_id))
    }

    async fn set_warehouse_deletion_profile_impl<'a>(
        warehouse_id: WarehouseId,
        deletion_profile: &TabularDeleteProfile,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<ResolvedWarehouse, SetWarehouseDeletionProfileError> {
        super::warehouse::update(
            warehouse_id,
            &mut transaction.txn,
            super::warehouse::set_delete_profile(deletion_profile),
        )
        .await
    }

    async fn set_warehouse_status_impl<'a>(
        warehouse_id: WarehouseId,
        status: WarehouseStatus,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<ResolvedWarehouse, SetWarehouseStatusError> {
        super::warehouse::update(
            warehouse_id,
            &mut transaction.txn,
            super::warehouse::set_status(status),
        )
        .await
    }

    async fn update_storage_profile_impl<'a>(
        warehouse_id: WarehouseId,
        storage_profile: StorageProfile,
        storage_secret_id: Option<SecretId>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<UpdatedWarehouseStorage, UpdateWarehouseStorageProfileError> {
        super::warehouse::update_storage_profile(
            warehouse_id,
            storage_profile,
            storage_secret_id,
            &mut transaction.txn,
        )
        .await
    }

    async fn set_warehouse_protected_impl(
        warehouse_id: WarehouseId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ResolvedWarehouse, SetWarehouseProtectedError> {
        super::warehouse::update(
            warehouse_id,
            &mut transaction.txn,
            super::warehouse::set_protected(protect),
        )
        .await
    }

    async fn set_warehouse_format_version_policy_impl(
        warehouse_id: WarehouseId,
        policy: &WarehouseFormatVersionPolicy,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ResolvedWarehouse, SetWarehouseFormatVersionPolicyError> {
        super::warehouse::update(
            warehouse_id,
            &mut transaction.txn,
            super::warehouse::set_format_version_policy(policy),
        )
        .await
    }

    async fn set_warehouse_managed_by_impl<'a>(
        warehouse_id: WarehouseId,
        managed_by: ManagedBy,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<ResolvedWarehouse, SetWarehouseManagedByError> {
        super::warehouse::update(
            warehouse_id,
            &mut transaction.txn,
            super::warehouse::set_managed_by(managed_by),
        )
        .await
    }

    async fn ensure_warehouse_spec_mutable_impl<'a>(
        warehouse_id: WarehouseId,
        bypass: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(), EnsureWarehouseSpecMutableError> {
        super::warehouse::ensure_spec_mutable(warehouse_id, bypass, &mut transaction.txn).await
    }

    async fn list_namespaces_impl<'a>(
        warehouse_id: WarehouseId,
        query: &ListNamespacesQuery,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<CatalogListNamespacesResponse, CatalogListNamespaceError> {
        super::namespace::list(warehouse_id, query, &mut transaction.txn).await
    }

    async fn create_namespace_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        request: CreateNamespaceRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<NamespaceWithParent, CatalogCreateNamespaceError> {
        super::namespace::create(warehouse_id, namespace_id, request, &mut transaction.txn).await
    }

    async fn get_namespaces_by_ident_impl<'a, 'b, SOT>(
        warehouse_id: WarehouseId,
        namespaces: &[&NamespaceIdent],
        state_or_transaction: &'b mut SOT,
    ) -> std::result::Result<Vec<NamespaceWithParent>, CatalogGetNamespaceError>
    where
        SOT: StateOrTransaction<
                Self::State,
                <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
            >,
        'a: 'b,
    {
        match state_or_transaction.as_enum_mut() {
            StateOrTransactionEnum::State(state) => {
                super::namespace::get_by_ident(warehouse_id, namespaces, &mut state.latest()).await
            }
            StateOrTransactionEnum::Transaction(t) => {
                super::namespace::get_by_ident(warehouse_id, namespaces, &mut t.txn).await
            }
        }
    }

    async fn get_namespaces_by_id_impl<'a, 'b, SOT>(
        warehouse_id: WarehouseId,
        namespaces: &[NamespaceId],
        state_or_transaction: &'b mut SOT,
    ) -> std::result::Result<Vec<NamespaceWithParent>, CatalogGetNamespaceError>
    where
        SOT: StateOrTransaction<
                Self::State,
                <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
            >,
        'a: 'b,
    {
        match state_or_transaction.as_enum_mut() {
            StateOrTransactionEnum::State(state) => {
                super::namespace::get_by_id(warehouse_id, namespaces, &mut state.latest()).await
            }
            StateOrTransactionEnum::Transaction(t) => {
                super::namespace::get_by_id(warehouse_id, namespaces, &mut t.txn).await
            }
        }
    }

    async fn drop_namespace_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        flags: NamespaceDropFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<NamespaceDropInfo, CatalogNamespaceDropError> {
        super::namespace::drop(warehouse_id, namespace_id, flags, &mut transaction.txn).await
    }

    async fn update_namespace_properties_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        updates: &HashMap<String, String>,
        removals: &[String],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<UpdatedNamespaceProperties, CatalogUpdateNamespacePropertiesError>
    {
        super::namespace::update_properties(
            warehouse_id,
            namespace_id,
            updates,
            removals,
            &mut transaction.txn,
        )
        .await
    }

    async fn set_namespace_protected_impl(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<NamespaceWithParent, CatalogSetNamespaceProtectedError> {
        super::namespace::set_protected(warehouse_id, namespace_id, protect, &mut transaction.txn)
            .await
    }

    async fn move_namespace_impl(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        destination: &NamespaceIdent,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<MovedNamespace, CatalogMoveNamespaceError> {
        super::namespace::move_to(
            warehouse_id,
            namespace_id,
            destination,
            force,
            &mut transaction.txn,
        )
        .await
    }

    async fn repair_namespace_path_casing_impl(
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<u64, CatalogBackendError> {
        Ok(0)
    }

    async fn repair_tabular_namespace_path_casing_impl(
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<u64, CatalogBackendError> {
        Ok(0)
    }

    async fn list_tabulars_impl(
        warehouse_id: WarehouseId,
        namespace_id: Option<NamespaceId>,
        list_flags: TabularListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
        typ: Option<TabularType>,
        pagination_query: PaginationQuery,
    ) -> std::result::Result<PaginatedMapping<TabularId, ViewOrTableDeletionInfo>, ListTabularsError>
    {
        super::tabular::list(
            warehouse_id,
            namespace_id,
            list_flags,
            &mut transaction.txn,
            typ,
            pagination_query,
        )
        .await
    }

    async fn search_tabular_impl(
        warehouse_id: WarehouseId,
        search_term: &str,
        catalog_state: Self::State,
    ) -> std::result::Result<CatalogSearchTabularResponse, SearchTabularError> {
        super::tabular::search(warehouse_id, search_term, &mut catalog_state.latest()).await
    }

    async fn set_tabular_protected_impl(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ViewOrTableInfo, SetTabularProtectionError> {
        super::tabular::set_protected(warehouse_id, tabular_id, protect, &mut transaction.txn).await
    }

    async fn get_tabular_infos_by_ident_impl(
        warehouse_id: WarehouseId,
        tabulars: &[TabularIdentBorrowed<'_>],
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> std::result::Result<HashMap<TableIdent, ViewOrTableInfo>, GetTabularInfoError> {
        super::tabular::get_by_ident(
            warehouse_id,
            tabulars,
            list_flags,
            &mut catalog_state.latest(),
        )
        .await
    }

    async fn get_tabular_infos_by_id_impl(
        warehouse_id: WarehouseId,
        tabulars: &[TabularId],
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> std::result::Result<Vec<ViewOrTableInfo>, GetTabularInfoError> {
        super::tabular::get_by_id(
            warehouse_id,
            tabulars,
            list_flags,
            &mut catalog_state.latest(),
        )
        .await
    }

    async fn get_tabular_infos_by_s3_location_impl(
        warehouse_id: WarehouseId,
        location: &Location,
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> std::result::Result<Option<ViewOrTableInfo>, GetTabularInfoByLocationError> {
        super::tabular::get_by_location(
            warehouse_id,
            location,
            list_flags,
            &mut catalog_state.latest(),
        )
        .await
    }

    async fn rename_tabular_impl(
        warehouse_id: WarehouseId,
        source_id: TabularId,
        source_namespace_id: NamespaceId,
        destination_namespace_id: NamespaceId,
        source: &TableIdent,
        destination: &TableIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ViewOrTableInfo, RenameTabularError> {
        super::tabular::rename(
            warehouse_id,
            source_id,
            source_namespace_id,
            destination_namespace_id,
            source,
            destination,
            &mut transaction.txn,
        )
        .await
    }

    async fn clear_tabular_deleted_at_impl(
        tabular_id: &[TabularId],
        warehouse_id: WarehouseId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<Vec<ViewOrTableDeletionInfo>, ClearTabularDeletedAtError> {
        super::tabular::clear_deleted_at(warehouse_id, tabular_id, None, &mut transaction.txn).await
    }

    async fn clear_tabular_deleted_at_for_tasks_impl(
        tabular_ids: &[TabularId],
        task_ids: &[TaskId],
        warehouse_id: WarehouseId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<Vec<ViewOrTableDeletionInfo>, ClearTabularDeletedAtError> {
        super::tabular::clear_deleted_at(
            warehouse_id,
            tabular_ids,
            Some(task_ids),
            &mut transaction.txn,
        )
        .await
    }

    async fn mark_tabular_as_deleted_impl(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ViewOrTableInfo, MarkTabularAsDeletedError> {
        super::tabular::mark_as_deleted(warehouse_id, tabular_id, force, &mut transaction.txn).await
    }

    async fn drop_tabular_impl<'a>(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Location, DropTabularError> {
        super::tabular::drop(warehouse_id, tabular_id, force, &mut transaction.txn).await
    }

    async fn create_table_impl<'a>(
        table_creation: TableCreation<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(TableInfo, Option<StagedTableId>), CreateTableError> {
        super::table::create(table_creation, &mut transaction.txn).await
    }

    async fn load_tables_impl<'a>(
        warehouse_id: WarehouseId,
        tables: impl IntoIterator<Item = TableId> + Send,
        include_deleted: bool,
        filters: &LoadTableFilters,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Vec<LoadTableResponse>, LoadTableError> {
        super::table::load(
            warehouse_id,
            tables,
            include_deleted,
            filters,
            &mut transaction.txn,
            &*transaction.files,
        )
        .await
    }

    async fn commit_table_transaction_impl<'a>(
        warehouse_id: WarehouseId,
        commits: impl IntoIterator<Item = TableCommit> + Send,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Vec<TableInfo>, CommitTableTransactionError> {
        super::table::commit(warehouse_id, commits, &mut transaction.txn).await
    }

    async fn create_view_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        view_ident: &TableIdent,
        request: &ViewMetadata,
        metadata_location: &Location,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<ViewInfo, CreateViewError> {
        super::view::create(
            warehouse_id,
            namespace_id,
            view_ident,
            request,
            metadata_location,
            &mut transaction.txn,
        )
        .await
    }

    async fn load_view_impl<'a>(
        warehouse_id: WarehouseId,
        view: ViewId,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<CatalogView, LoadViewError> {
        super::view::load(
            warehouse_id,
            view,
            include_deleted,
            &mut transaction.txn,
            &*transaction.files,
        )
        .await
    }

    async fn commit_view_impl<'a>(
        commit: ViewCommit<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<ViewInfo, CommitViewError> {
        super::view::commit(commit, &mut transaction.txn).await
    }

    async fn create_generic_table_impl<'a>(
        creation: GenericTableCreation,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<GenericTableInfo, CreateGenericTableError> {
        super::generic_table::create(creation, &mut transaction.txn).await
    }

    async fn load_generic_table_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        table_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<GenericTableInfo, LoadGenericTableError> {
        super::generic_table::load(warehouse_id, namespace_id, table_name, &mut transaction.txn)
            .await
    }

    async fn load_generic_table_by_id_impl<'a>(
        warehouse_id: WarehouseId,
        generic_table_id: GenericTableId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<GenericTableInfo, LoadGenericTableError> {
        super::generic_table::load_by_id(warehouse_id, generic_table_id, &mut transaction.txn).await
    }

    async fn list_generic_tables_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        namespace_ident: &lakekeeper::iceberg::NamespaceIdent,
        page_size: Option<i64>,
        page_token: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<
        PaginatedMapping<GenericTableId, GenericTableListEntry>,
        ListGenericTablesError,
    > {
        super::generic_table::list(
            warehouse_id,
            namespace_id,
            namespace_ident,
            page_size,
            page_token,
            &mut transaction.txn,
        )
        .await
    }

    async fn drop_generic_table_impl<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        table_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<GenericTableId, DropGenericTableError> {
        super::generic_table::drop(warehouse_id, namespace_id, table_name, &mut transaction.txn)
            .await
    }

    async fn create_roles_impl<'a>(
        project_id: &ProjectId,
        roles_to_create: Vec<CatalogCreateRoleRequest<'_>>,
        on_conflict: OnRoleConflict,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Vec<Role>, CreateRoleError> {
        Err(unsupported("create_roles_impl").into())
    }

    async fn update_role_impl<'a>(
        project_id: &ProjectId,
        role_id: RoleId,
        role_name: &str,
        description: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Role, UpdateRoleError> {
        Err(unsupported("update_role_impl").into())
    }

    async fn set_role_source_system_impl<'a>(
        project_id: &ProjectId,
        role_id: RoleId,
        request: &UpdateRoleSourceSystemRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<ReboundRole, UpdateRoleError> {
        Err(unsupported("set_role_source_system_impl").into())
    }

    async fn list_roles_impl(
        project_id: Option<&ProjectId>,
        filter: CatalogListRolesByIdFilter<'_>,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListRolesResponse, ListRolesError> {
        Err(unsupported("list_roles_impl").into())
    }

    async fn delete_roles_impl<'a>(
        project_id: Option<&ProjectId>,
        filter: CatalogListRolesByIdFilter<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Vec<RoleId>, CatalogBackendError> {
        Err(unsupported("delete_roles_impl"))
    }

    async fn lock_role_and_count_grants_impl<'a>(
        project_id: &ProjectId,
        role_id: RoleId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<u64, DeleteRoleError> {
        Err(unsupported("lock_role_and_count_grants_impl").into())
    }

    async fn search_role_impl(
        project_id: &ProjectId,
        search_term: &str,
        catalog_state: Self::State,
    ) -> Result<SearchRoleResponse, SearchRolesError> {
        Err(unsupported("search_role_impl").into())
    }

    async fn list_roles_by_idents_in_projects_impl(
        project_ids: &[&ProjectId],
        idents: &[&RoleIdent],
        catalog_state: Self::State,
    ) -> Result<Vec<Role>, CatalogBackendError> {
        Err(unsupported("list_roles_by_idents_in_projects_impl"))
    }

    async fn apply_grants_impl<'a>(
        writes: &[GrantSpec],
        deletes: &[GrantSpec],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<AppliedGrants, ApplyGrantsStoreError> {
        Err(unsupported("apply_grants_impl").into())
    }

    async fn insert_grants_impl<'a>(
        writes: &[GrantSpec],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Vec<GrantSpec>, ApplyGrantsStoreError> {
        Err(unsupported("insert_grants_impl").into())
    }

    async fn delete_grants_for_user_impl<'a>(
        user_id: &UserId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Vec<GrantSpec>, ApplyGrantsStoreError> {
        Err(unsupported("delete_grants_for_user_impl").into())
    }

    async fn list_grants_impl(
        filter: &GrantFilter,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListGrantsResultPage, ListGrantsStoreError> {
        Err(unsupported("list_grants_impl").into())
    }

    async fn list_grants_in_subtree_impl(
        root: SubtreeGrantRoot,
        filter: &SubtreeGrantFilter,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListSubtreeGrantsResultPage, ListGrantsStoreError> {
        Err(unsupported("list_grants_in_subtree_impl").into())
    }

    async fn count_subtree_namespaces_impl(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        catalog_state: Self::State,
    ) -> Result<u64, ListGrantsStoreError> {
        Err(unsupported("count_subtree_namespaces_impl").into())
    }

    async fn select_subtree_grant_candidates_impl(
        root: SubtreeGrantRoot,
        filter: &SubtreeGrantFilter,
        limit: usize,
        catalog_state: Self::State,
    ) -> Result<GrantRevokeCandidates, ListGrantsStoreError> {
        Err(unsupported("select_subtree_grant_candidates_impl").into())
    }

    async fn revoke_grant_candidates_impl<'a>(
        root: SubtreeGrantRoot,
        candidates: &[GrantCandidate],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Vec<GrantSpec>, RevokeSubtreeGrantsStoreError> {
        Err(unsupported("revoke_grant_candidates_impl").into())
    }

    async fn list_grants_on_resources_impl(
        principals: &[UserOrRoleId],
        resources: &[GrantResource],
        catalog_state: Self::State,
    ) -> Result<Vec<GrantSpec>, ListGrantsStoreError> {
        Err(unsupported("list_grants_on_resources_impl").into())
    }

    async fn create_tag_definition_impl<'a>(
        project_id: &ProjectId,
        request: CatalogCreateTagDefinitionRequest<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<TagDefinition, CreateTagDefinitionError> {
        Err(unsupported("create_tag_definition_impl").into())
    }

    async fn get_tag_definition_impl(
        project_id: &ProjectId,
        tag_definition_id: TagDefinitionId,
        catalog_state: Self::State,
    ) -> Result<Option<TagDefinition>, CatalogBackendError> {
        Err(unsupported("get_tag_definition_impl"))
    }

    async fn get_tag_definition_by_name_impl(
        project_id: &ProjectId,
        name: &str,
        catalog_state: Self::State,
    ) -> Result<Option<TagDefinition>, CatalogBackendError> {
        Err(unsupported("get_tag_definition_by_name_impl"))
    }

    async fn list_tag_definitions_impl(
        project_id: &ProjectId,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListTagDefinitionsResponse, ListTagDefinitionsError> {
        Err(unsupported("list_tag_definitions_impl").into())
    }

    async fn get_tag_allowed_values_impl(
        tag_definition_id: TagDefinitionId,
        catalog_state: Self::State,
    ) -> Result<Vec<String>, CatalogBackendError> {
        Err(unsupported("get_tag_allowed_values_impl"))
    }

    async fn update_tag_definition_impl<'a>(
        project_id: &ProjectId,
        tag_definition_id: TagDefinitionId,
        request: UpdateTagDefinitionRequest<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(TagDefinition, Vec<String>), UpdateTagDefinitionError> {
        Err(unsupported("update_tag_definition_impl").into())
    }

    async fn delete_tag_definition_impl<'a>(
        project_id: &ProjectId,
        tag_definition_id: TagDefinitionId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(), DeleteTagDefinitionError> {
        Err(unsupported("delete_tag_definition_impl").into())
    }

    async fn apply_tag_impl<'a>(
        tag_id: TagId,
        tag_definition_id: TagDefinitionId,
        target: TagTarget,
        value: Option<&str>,
        source: TagSource,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(Tag, bool), ApplyTagError> {
        Err(unsupported("apply_tag_impl").into())
    }

    async fn remove_tag_impl<'a>(
        tag_id: TagId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(), RemoveTagError> {
        Err(unsupported("remove_tag_impl").into())
    }

    async fn remove_tag_for_target_impl<'a>(
        target: TagTarget,
        tag_definition_id: TagDefinitionId,
        source: TagSource,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<Tag>, RemoveTagError> {
        Err(unsupported("remove_tag_for_target_impl").into())
    }

    async fn list_tags_for_target_impl(
        target: TagTarget,
        catalog_state: Self::State,
    ) -> Result<Vec<TagWithName>, CatalogBackendError> {
        Err(unsupported("list_tags_for_target_impl"))
    }

    async fn list_tags_on_targets_impl(
        targets: &[TagTarget],
        catalog_state: Self::State,
    ) -> Result<Vec<TagWithName>, CatalogBackendError> {
        Err(unsupported("list_tags_on_targets_impl"))
    }

    async fn list_column_tags_for_tabular_impl(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        catalog_state: Self::State,
    ) -> Result<Vec<TagWithName>, CatalogBackendError> {
        Err(unsupported("list_column_tags_for_tabular_impl"))
    }

    async fn list_tag_attachments_impl(
        tag_definition_id: TagDefinitionId,
        filter: &TagAttachmentFilter,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListTagAttachmentsResponse, ListTagAttachmentsError> {
        Err(unsupported("list_tag_attachments_impl").into())
    }

    async fn list_effective_tag_candidates_impl(
        target: TagTarget,
        catalog_state: Self::State,
    ) -> Result<Vec<EffectiveTagCandidate>, CatalogBackendError> {
        Err(unsupported("list_effective_tag_candidates_impl"))
    }

    async fn sync_role_members_by_ident_impl<'a>(
        project_id: &ProjectId,
        role: &CatalogRoleForAssignment<'_>,
        members: &[CatalogUserRoleAssignmentUser<'_>],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<SyncRoleMembersResult, SyncRoleMembersError> {
        Err(unsupported("sync_role_members_by_ident_impl").into())
    }

    async fn sync_user_role_assignments_by_provider_impl<'a>(
        user: &CatalogUserRoleAssignmentUser<'_>,
        sync_for: SyncFor,
        project_id: &ProjectId,
        provider_id: &RoleProviderId,
        roles: &[CatalogRoleForAssignment<'_>],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<SyncUserRoleAssignmentsResult, SyncUserRoleAssignmentsError> {
        Err(unsupported("sync_user_role_assignments_by_provider_impl").into())
    }

    async fn list_role_assignments_for_user_impl(
        user_id: &UserId,
        catalog_state: Self::State,
    ) -> Result<ListUserRoleAssignmentsResult, CatalogBackendError> {
        Err(unsupported("list_role_assignments_for_user_impl"))
    }

    async fn list_role_assignments_for_role_impl(
        role_id: RoleId,
        catalog_state: Self::State,
    ) -> Result<Option<ListRoleMembersResult>, CatalogBackendError> {
        Err(unsupported("list_role_assignments_for_role_impl"))
    }

    async fn list_role_ancestors_impl(
        role_ids: &[RoleId],
        catalog_state: Self::State,
    ) -> Result<HashMap<RoleId, Vec<AssignedRole>>, CatalogBackendError> {
        Err(unsupported("list_role_ancestors_impl"))
    }

    async fn list_direct_role_assignments_for_user_impl(
        user_id: &UserId,
        catalog_state: Self::State,
    ) -> Result<Vec<AssignedRole>, CatalogBackendError> {
        Err(unsupported("list_direct_role_assignments_for_user_impl"))
    }

    async fn list_role_assignments_for_role_by_ident_impl(
        project_id: &ProjectId,
        role_ident: &RoleIdent,
        catalog_state: Self::State,
    ) -> Result<Option<ListRoleMembersResult>, CatalogBackendError> {
        Err(unsupported("list_role_assignments_for_role_by_ident_impl"))
    }

    async fn add_role_members_impl<'a>(
        project_id: &ArcProjectId,
        parent_role_id: RoleId,
        member_role_ids: &[RoleId],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<AddRoleMembersResult, AddRoleMembersError> {
        Err(unsupported("add_role_members_impl").into())
    }

    async fn remove_role_members_impl<'a>(
        parent_role_id: RoleId,
        member_role_ids: &[RoleId],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<RemoveRoleMembersResult, RemoveRoleMembersError> {
        Err(unsupported("remove_role_members_impl").into())
    }

    async fn add_user_role_assignments_impl<'a>(
        project_id: &ArcProjectId,
        role_id: RoleId,
        user_ids: &[UserId],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<AddUserRoleAssignmentsResult, AddUserRoleAssignmentsError> {
        Err(unsupported("add_user_role_assignments_impl").into())
    }

    async fn remove_user_role_assignments_impl<'a>(
        role_id: RoleId,
        user_ids: &[UserId],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<RemoveUserRoleAssignmentsResult, RemoveUserRoleAssignmentsError> {
        Err(unsupported("remove_user_role_assignments_impl").into())
    }

    async fn list_role_memberships_impl(
        role_id: RoleId,
        direction: RoleMembershipDirection,
        catalog_state: Self::State,
    ) -> Result<Vec<RoleMembershipEntry>, CatalogBackendError> {
        Err(unsupported("list_role_memberships_impl"))
    }

    async fn affected_users_for_membership_edges_impl<'a>(
        member_role_ids: &[RoleId],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Vec<UserId>, CatalogBackendError> {
        Err(unsupported("affected_users_for_membership_edges_impl"))
    }

    async fn expire_role_assignment_syncs_impl<'a>(
        project_id: &ProjectId,
        provider_id: &RoleProviderId,
        user_ids: &[UserId],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(), CatalogBackendError> {
        Err(unsupported("expire_role_assignment_syncs_impl"))
    }

    async fn list_direct_role_members_page(
        project_id: &ProjectId,
        role_id: RoleId,
        type_filter: Option<RoleMemberKind>,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListCatalogRoleMembersPage> {
        Err(ErrorModel::from(unsupported("list_direct_role_members_page")).into())
    }

    async fn list_direct_role_member_of_page(
        project_id: &ProjectId,
        role_id: RoleId,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListRolesPage> {
        Err(ErrorModel::from(unsupported("list_direct_role_member_of_page")).into())
    }

    async fn list_direct_user_roles_page(
        project_id: &ProjectId,
        user_id: &UserId,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<Option<ListRolesPage>> {
        Err(ErrorModel::from(unsupported("list_direct_user_roles_page")).into())
    }

    async fn list_transitive_role_members_page(
        project_id: &ProjectId,
        role_id: RoleId,
        type_filter: Option<RoleMemberKind>,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListCatalogRoleMembersPage> {
        Err(ErrorModel::from(unsupported("list_transitive_role_members_page")).into())
    }

    async fn list_transitive_user_roles_page(
        project_id: &ProjectId,
        user_id: &UserId,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<Option<ListRolesPage>> {
        Err(ErrorModel::from(unsupported("list_transitive_user_roles_page")).into())
    }

    async fn list_transitive_role_member_of_page(
        project_id: &ProjectId,
        role_id: RoleId,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListRolesPage> {
        Err(ErrorModel::from(unsupported("list_transitive_role_member_of_page")).into())
    }

    async fn list_user_membership_entries(
        user_ids: &[UserId],
        catalog_state: Self::State,
    ) -> Result<Vec<UserMembershipEntry>> {
        Err(ErrorModel::from(unsupported("list_user_membership_entries")).into())
    }

    async fn create_or_update_user<'a>(
        user_id: &UserId,
        name: &str,

        email: Option<&str>,
        last_updated_with: UserLastUpdatedWith,
        user_type: UserType,
        mode: UserUpsertMode,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<CreateOrUpdateUserResponse> {
        super::user::create_or_update(
            user_id,
            name,
            email,
            last_updated_with,
            user_type,
            mode,
            &mut transaction.txn,
        )
        .await
    }

    async fn search_user(
        search_term: &str,
        catalog_state: Self::State,
    ) -> Result<SearchUserResponse> {
        super::user::search(search_term, &mut catalog_state.latest()).await
    }

    async fn list_user(
        filter_user_id: Option<Vec<UserId>>,
        filter_name: Option<String>,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListUsersResponse> {
        super::user::list(
            filter_user_id,
            filter_name,
            pagination,
            &mut catalog_state.latest(),
        )
        .await
    }

    async fn delete_user_impl<'a>(
        user_id: UserId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Option<DeletedUser>> {
        super::user::delete(&user_id, &mut transaction.txn).await
    }

    async fn get_endpoint_statistics(
        project_id: ArcProjectId,
        warehouse_id: WarehouseFilter,
        range_specifier: TimeWindowSelector,
        status_codes: Option<&[u16]>,
        catalog_state: Self::State,
    ) -> Result<EndpointStatisticsResponse> {
        super::endpoint_statistics::get(range_specifier)
    }

    async fn pick_new_task_impl(
        request: PickTaskRequest<'_>,
        state: Self::State,
    ) -> Result<Option<Task>> {
        Ok(super::tasks::pick_new_task(request, &state).await?)
    }

    async fn task_queue_demand_impl(
        request: TaskQueueDemandRequest<'_>,
        state: Self::State,
    ) -> std::result::Result<TaskQueueDemand, CatalogBackendError> {
        super::tasks::task_queue_demand(request, &state).await
    }

    async fn task_queue_oldest_due_impl(
        request: TaskQueueDemandRequest<'_>,
        state: Self::State,
    ) -> std::result::Result<Option<chrono::DateTime<chrono::Utc>>, CatalogBackendError> {
        super::tasks::task_queue_oldest_due(request, &state).await
    }

    async fn resolve_tasks_impl(
        scope: TaskResolveScope,
        task_ids: &[TaskId],
        state: Self::State,
    ) -> Result<Vec<ResolvedTask>, ResolveTasksError> {
        super::task_admin::resolve_tasks(scope, task_ids, &mut state.latest()).await
    }

    async fn record_task_success_impl(
        id: TaskAttemptId,
        message: Option<&str>,
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Ok(super::tasks::record_success(id, message, &mut transaction.txn).await?)
    }

    async fn record_task_failure_impl(
        id: TaskAttemptId,
        error_details: &str,
        max_retries: i32,
        retry_decision: &RetryDecision,
        transaction: &mut <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Ok(super::tasks::record_failure(
            id,
            error_details,
            max_retries,
            retry_decision,
            &mut transaction.txn,
        )
        .await?)
    }

    async fn get_task_details_impl(
        task_id: TaskId,
        scope: TaskDetailsScope,
        num_attempts: u16,
        state: Self::State,
    ) -> Result<Option<TaskDetails>, GetTaskDetailsError> {
        super::task_admin::get_task_details(task_id, scope, num_attempts, &mut state.latest()).await
    }

    async fn list_tasks_impl(
        filter: &TaskFilter,
        query: &ListTasksRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<TaskList> {
        Ok(super::task_admin::list_tasks(filter, query, &mut transaction.txn).await?)
    }

    async fn enqueue_tasks_impl(
        queue_name: &'static TaskQueueName,
        tasks: Vec<TaskInput>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<TaskId>> {
        Ok(super::tasks::enqueue_tasks(queue_name, tasks, &mut transaction.txn).await?)
    }

    async fn enqueue_task_impl(
        queue_name: &'static TaskQueueName,
        task: TaskInput,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<EnqueueOutcome> {
        Ok(super::tasks::enqueue_task(queue_name, task, &mut transaction.txn).await?)
    }

    async fn reparent_pending_task_impl(
        queue_name: &TaskQueueName,
        legacy_queue_names: &[&TaskQueueName],
        task_id: TaskId,
        new_parent: TaskId,
        task_data: serde_json::Value,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<bool> {
        Ok(super::task_admin::reparent_pending_task(
            queue_name,
            legacy_queue_names,
            task_id,
            new_parent,
            &task_data,
            &mut transaction.txn,
        )
        .await?)
    }

    async fn upsert_task_schedule_impl(
        schedule: TaskScheduleUpsert,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<TaskSchedule, UpsertTaskScheduleError> {
        super::tasks::upsert_task_schedule(schedule, &mut transaction.txn).await
    }

    async fn list_task_schedules_impl(
        filter: &TaskScheduleFilter,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<TaskSchedule>, TaskScheduleStoreError> {
        super::tasks::list_task_schedules(filter, &mut transaction.txn).await
    }

    async fn delete_task_schedule_impl(
        schedule_id: TaskScheduleId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<bool, TaskScheduleStoreError> {
        super::tasks::delete_task_schedule(schedule_id, &mut transaction.txn).await
    }

    async fn list_project_task_schedule_states_impl(
        queue_name: &TaskQueueName,
        schedule_key: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<ProjectTaskScheduleState>, TaskScheduleStoreError> {
        super::tasks::list_project_task_schedule_states(
            queue_name,
            schedule_key,
            &mut transaction.txn,
        )
        .await
    }

    async fn delete_warehouse_task_schedules_impl(
        warehouse_id: WarehouseId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<(), TaskScheduleStoreError> {
        super::tasks::delete_warehouse_task_schedules(warehouse_id, &mut transaction.txn).await
    }

    async fn trigger_task_schedule_impl(
        schedule_id: TaskScheduleId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<EnqueueOutcome, TriggerTaskScheduleError> {
        super::tasks::trigger_task_schedule(schedule_id, &mut transaction.txn).await
    }

    async fn materialize_due_task_schedules_impl(
        limit: i64,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<MaterializedTick>, TaskScheduleStoreError> {
        super::tasks::materialize_due_task_schedules(limit, &mut transaction.txn).await
    }

    async fn cancel_scheduled_tasks_impl(
        queue_name: Option<&TaskQueueName>,
        legacy_queue_names: &[&TaskQueueName],
        filter: CancelTasksFilter,
        cancel_running_and_should_stop: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Ok(super::tasks::cancel_scheduled_tasks(
            queue_name,
            legacy_queue_names,
            filter,
            cancel_running_and_should_stop,
            &mut transaction.txn,
        )
        .await?)
    }

    async fn check_and_heartbeat_task_impl(
        id: TaskAttemptId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
        progress: f32,
        execution_details: Option<serde_json::Value>,
    ) -> Result<TaskCheckState> {
        Ok(
            super::tasks::check_and_heartbeat(
                id,
                progress,
                execution_details,
                &mut transaction.txn,
            )
            .await?,
        )
    }

    async fn stop_tasks_impl(
        task_ids: &[TaskId],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Ok(super::task_admin::stop_tasks(task_ids, &mut transaction.txn).await?)
    }

    async fn run_tasks_at_impl(
        task_ids: &[TaskId],
        scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Ok(super::task_admin::run_tasks_at(task_ids, scheduled_for, &mut transaction.txn).await?)
    }

    async fn set_task_queue_config_impl(
        project_id: ArcProjectId,
        warehouse_id: Option<WarehouseId>,
        queue_name: &TaskQueueName,
        config: &SetTaskQueueConfigRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Ok(super::tasks::set_task_queue_config(
            &project_id,
            warehouse_id,
            queue_name,
            config,
            &mut transaction.txn,
        )
        .await?)
    }

    async fn get_task_queue_config_impl(
        filter: &TaskQueueConfigFilter,
        queue_name: &TaskQueueName,
        state: Self::State,
    ) -> Result<Option<GetTaskQueueConfigResponse>> {
        Ok(super::tasks::get_task_queue_config(filter, queue_name, &mut state.latest()).await?)
    }

    async fn cleanup_task_logs_older_than(
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
        retention_period: Duration,
        project_id: &ProjectId,
    ) -> Result<()> {
        Ok(super::tasks::cleanup_task_logs_older_than(
            &mut transaction.txn,
            retention_period,
            project_id,
        )
        .await?)
    }

    async fn check_idempotency_key_impl(
        warehouse_id: WarehouseId,
        key: &lakekeeper::service::idempotency::IdempotencyKey,
        endpoint: lakekeeper::api::endpoints::EndpointFlat,
        state: Self::State,
    ) -> Result<lakekeeper::service::idempotency::IdempotencyCheck> {
        super::idempotency::check(&mut state.latest(), warehouse_id, key, endpoint).await
    }

    async fn try_insert_idempotency_key_impl<'a>(
        warehouse_id: WarehouseId,
        info: &lakekeeper::service::idempotency::IdempotencyInfo,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<bool> {
        super::idempotency::try_insert(&mut transaction.txn, warehouse_id, info).await
    }
}
