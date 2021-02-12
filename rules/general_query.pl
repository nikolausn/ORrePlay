%state(N) :- N=-1.
%dataset_name(N) :- N="airbnb_dirty-csv.openrefine-2.tar.gz".
%state(-1,none,none).

content(-1,nul,nul,nul,nul).

change_content_at_before_state(PrevContentId, StateId) :-
    content(ContentId, _, NextColumnSchemaStateId, _, PrevContentId),
    NextColumnSchemaStateId <= StateId,
    state_num(StateId).

change_content_at_state(ContentId, CellId, StateId) :-
    content(ContentId, CellId, AssignmentStateId, _, _),
    StateId >= AssignmentStateId,
    not change_content_at_before_state(ContentId, StateId),
    state_num(StateId).

change_content_at_state_test(ContentId,CellId) :-
change_content_at_state(ContentId,CellId,StateId),
state(StateId).

#show change_content_at_state_test/2.

change_count_at_before_state(B) :- content(A,B,N,_,_),
    state(N).

change_count_at_state(B) :- content(A,B,N,_,_),
    state(N).


%state_edge(PrevStateId,StateId,NextStateId) :- 
%    state(StateId,ImportStateId,PrevStateId),
%    state(NextStateId,_,StateId).
%#show state_edge/3.
%#show state_edge/2.
%#show import_state/4.
%#show state_command/3.

% next column
next_column(ColSchemaId,NextColSchemaId,PrevCol,NextColId,StateId,NextStateId) :-  
    column_schema(ColSchemaId, ColumnId, StateId, _, _, _, _),
    column_schema(NextColSchemaId, ColumnId, NextStateId, _, _, PrevCol, ColSchemaId),
    NextColId=-1..M,
    M=#max{A:column(A,_)}.

% Column Name

% all column at before state
all_column_schema_at_before_state(PrevColSchemaId, StateId) :-
    column_schema(ColumnSchemaId, _, NextColumnSchemaStateId, _, _, _, PrevColSchemaId),
    NextColumnSchemaStateId <= StateId,
    %StateId=#max{A: state(A, _, _)}.
    state_num(StateId).

state_num(-1..M):- M=#max{A: state(A, _, _)}.

state_edge(state_name(PrevStateId,StateId,CommandName),state_name(StateId,NextStateId,NextCommandName)) :- 
    state(StateId,ImportStateId,PrevStateId),
    state(NextStateId,_,StateId),
    %state(NextStateId,_,PrevStateId),
    import_state(DatasetName, _, _, ImportStateId),
    state_command(StateId,_,CommandName),
    state_command(NextStateId,_,NextCommandName),
    dataset_name(DatasetName).


%#show state/3.

all_column_schema_at_state(ColumnSchemaId, ColumnId, StateId) :-
    column_schema(ColumnSchemaId, ColumnId, AssignmentStateId, _, _, _, _),
    StateId >= AssignmentStateId,
    not all_column_schema_at_before_state(ColumnSchemaId, StateId),
    %StateId=-1..M,
    %M=#max{A: state(A, _, _)}.
    state_num(StateId).

% schema name at state
all_column_name_at_state(StateId,ColumnId,ColumnName,PrevColId,ColumnSchemaId) :-
    all_column_schema_at_state(ColumnSchemaId, ColumnId, StateId),
    column_schema(ColumnSchemaId,_,_,_,ColumnName,PrevColId,_).

all_column_name_at_state_test(StateId,ColumnId,ColumnName,PrevCol,ColSchemaId) :-
    all_column_name_at_state(StateId,ColumnId,ColumnName,PrevCol,ColSchemaId),
    state(StateId).

next_row_position_at_or_before_state(RowPositionId, StateId) :-
    row_position(_, _, NextRowPositionStateId, RowPositionId),
    %NextRowPositionStateId <= StateId,
    NextRowPositionStateId <= StateId,
    state_num(StateId).

num(N):- {node(X)} == N. % count nodes
step(1..N) :- num(N). % mark possible steps

%row_position_at_state(RowPosId, RowId, StateId) :-
%    row_position(RowPosId, RowId, AssignmentStateId, _),
%    next_row_position_at_or_before_state(RowPosId, StateId),
    %StateId = #min{A:next_row_position_at_or_before_state(_,A)},
    %not next_row_position_at_or_before_state(RowPosId, StateId),
%    state_num(StateId).

row_position_state(StateId,AssignmentStateId):-
    row_position(RowPosId, RowId, AssignmentStateId, _),
    AssignmentStateId <= StateId,
    state_num(StateId).

max_row_position_state(StateId,AssignmentStateId):-
    row_position_state(StateId,AssignmentStateId),
    AssignmentStateId = #max{A:row_position_state(StateId,A)},
    state_num(StateId).

row_position_at_state(RowPosId, RowId, StateId,PrevRowId) :-
    row_position(RowPosId, RowId, AssignmentStateId, _),
    max_row_position_state(StateId,AssignmentStateId),
    row_position(RowPosId,_,_,PrevRowId),
    state_num(StateId).

row_position_at_state_test(RowPosId, RowId, StateId,PrevRowId) :-
    row_position_at_state(RowPosId, RowId, StateId,PrevRowId),
    %row_position(RowPosId,_,_,PrevRowId),
    state(StateId).

row_position_at_state(nul,-1,nul,nul).

% get all import state
import_state(SourceUri, DatasetId, ImportArrayId, ImportStateId) :-
    source(SourceId, SourceUri, _),
    % dataset(DatasetId, SourceId, ImportArrayId),
    dataset(DatasetId, SourceId),
    array(DatasetId,ImportArrayId),
    state(ImportStateId, ImportArrayId, -1).

% get all column rename
column_rename(DatasetId, ArrayId, StateId, ColumnName, NewColumnName) :-
    array(ArrayId, DatasetId),
    state(StateId, ArrayId, _),
    column_schema(PreviousColumnSchemaId, _, _, _, NewColumnName, _, _),
    column_schema(_, _, StateId, _, ColumnName, _, PreviousColumnSchemaId),
    NewColumnName <> ColumnName.

% get column schema before state
next_column_schema_at_or_before_state(ColumnSchemaId, StateId) :-
    column_schema(_, _, NextColumnSchemaStateId, _, _, _, ColumnSchemaId),
    NextColumnSchemaStateId <= StateId,
    StateId=#max{A: state(A, _, _)}.

% get latest column schema at state
column_schema_at_state(ColumnSchemaId, ColumnId, StateId) :-
    column_schema(ColumnSchemaId, ColumnId, AssignmentStateId, _, _, _, _),
    StateId >= AssignmentStateId,
    not next_column_schema_at_or_before_state(ColumnSchemaId, StateId),
    StateId=-1..M,
    M=#max{A: state(A, _, _)}.

column_name(ColumnSchemaId, ColumnName) :-
    column_schema(ColumnSchemaId, _, _, _, ColumnName, _, _).
column_name(ColumnId, StateId, ColumnName) :-
    column_schema_at_state(ColumnSchemaId, ColumnId, StateId),
    column_schema(ColumnSchemaId, _, _, _, ColumnName, _, _).
% RULE : schema_of_previous_column/3 - returns ID of schema for column at left as of StateId
schema_of_previous_column(ColumnSchemaId, SchemaIdForPreviousColumn, StateId) :-
    column_schema(ColumnSchemaId, _, _, _, _, PreviousColumnId, _),
    column_schema_at_state(SchemaIdForPreviousColumn, PreviousColumnId, StateId).

column_schema_at_state_not(SchemaIdOfNextColumn, ColumnId, StateId):-
    column_schema_at_state(SchemaIdOfNextColumn, _, StateId),
        column_schema(SchemaIdOfNextColumn, _, _, _, _, ColumnId, _).

last_column(StateId, ColumnId, ColumnSchemaId) :-
    column_schema_at_state(ColumnSchemaId, ColumnId, StateId),
    not column_schema_at_state_not(_, _, StateId).

final_array_state(ArrayId, StateId) :-
    state(StateId, ArrayId, _),
    not state(_, ArrayId, StateId).

column_schema_at_state_not_test(SchemaIdOfNextColumn, ColumnId, StateId):-
    column_schema_at_state_not(SchemaIdOfNextColumn, ColumnId, StateId),
    StateId=3.

column_schema_at_state_test(SchemaIdOfNextColumn, ColumnId, StateId):-
    column_schema_at_state(SchemaIdOfNextColumn, ColumnId, StateId),
    StateId=3.

column_name_state_test(ColumnId, StateId, ColumnName) :-
    column_name(ColumnId, StateId, ColumnName), StateId=5.
