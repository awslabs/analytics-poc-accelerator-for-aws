export const WORKGROUP_READ_ACTIONS = [
  "athena:GetDatabase",
  "athena:GetTableMetadata",
  "athena:ListDatabases",
  "athena:ListDataCatalogs",
  "athena:ListEngineVersions",
  "athena:ListTableMetadata",
  "athena:ListWorkGroups",
];

export const WORKGROUP_RUN_QUERY_ACTIONS = [
  "athena:BatchGetNamedQuery",
  "athena:BatchGetQueryExecution",
  "athena:CreateNamedQuery",
  "athena:CreatePreparedStatement",
  "athena:DeleteNamedQuery",
  "athena:DeletePreparedStatement",
  "athena:GetNamedQuery",
  "athena:GetPreparedStatement",
  "athena:GetQueryExecution",
  "athena:GetQueryResults",
  "athena:GetQueryResultsStream",
  "athena:GetQueryRuntimeStatistics",
  "athena:GetWorkGroup",
  "athena:ListNamedQueries",
  "athena:ListPreparedStatements",
  "athena:ListQueryExecutions",
  "athena:StartQueryExecution",
  "athena:StopQueryExecution",
  "athena:UpdatePreparedStatement",
];

export const WORKGROUP_MANAGE_ACTIONS = [
  "athena:CreateWorkGroup",
  "athena:DeleteWorkGroup",
  "athena:GetWorkGroup",
  "athena:UpdateWorkGroup",
];
