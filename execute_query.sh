state=3

echo "What is the name of dataset ?"
cat <<EOT > temp_runner.pl
state(N) :- N=$state.
#show q1/1.
EOT
#clingo 03_poster_demo.openrefine.extract/facts.pl rules/column_query.pl  temp_runner.pl
#clingo airbnb_dirty-csv.openrefine-2.extract/facts.pl rules/column_query.pl  temp_runner.pl

echo

echo "Column Order ?"
cat <<EOT > temp_runner.pl
state(N) :- N=0.
% all column at before state
all_column_schema_at_before_state(PrevColSchemaId, StateId) :-
    column_schema(ColumnSchemaId, _, NextColumnSchemaStateId, _, _, _, PrevColSchemaId),
    NextColumnSchemaStateId <= StateId,
    %StateId=#max{A: state(A, _, _)}.
    state_num(StateId).

state_num(-1..M):- M=#max{A: state(A, _, _)}.

all_column_schema_at_state(ColumnSchemaId, ColumnId, StateId) :-
    column_schema(ColumnSchemaId, ColumnId, AssignmentStateId, _, _, _, _),
    StateId >= AssignmentStateId,
    not all_column_schema_at_before_state(ColumnSchemaId, StateId),
    %StateId=-1..M,
    %M=#max{A: state(A, _, _)}.
    state_num(StateId).

column_schema_at_state(ColumnSchemaId,StateId,ColumnId,ColumnName,PrevColumnId):-
    all_column_schema_at_state(ColumnSchemaId, ColumnId, StateId),
    column_schema(ColumnSchemaId,ColumnId,_,_,ColumnName,PrevColumnId,_),
    state(StateId).

column_lineage(StateId,0,ColumnId,-1):-
    column_schema_at_state(_,StateId,ColumnId,_,-1).
column_lineage(StateId,Level+1,ColumnId,ParentColumnId):-
    column_lineage(StateId,Level,ParentColumnId,_),
    column_schema_at_state(_,StateId,ColumnId,_,ParentColumnId),
    Level<10,
    state_num(StateId). 

#show column_schema_at_state/5.
#show column_lineage/4.
EOT
clingo ipaw_2021_demo.openrefine.3.extract/facts.pl  temp_runner.pl
#clingo airbnb_dirty-csv.openrefine-2.extract/facts.pl rules/column_query.pl  temp_runner.pl



echo "State Lineage ?"
cat <<EOT > temp_runner.pl
state(N) :- N=$state.
%state_lineage_remove_dup(StateId,MaxLevel,Mediator,CommonAncestor):-
%    state_lineage(StateId,_,Mediator,CommonAncestor),
%    MaxLevel = #max{N:state_lineage(StateId,N,_,CommonAncestor)}.

state_lineage_remove_dup(StateId,MaxLevel,CommonAncestor):-
    state_lineage(StateId,_,_,CommonAncestor),
    MaxLevel = #max{N:state_lineage(StateId,N,_,CommonAncestor)}.

%state_lineage_test(StateId,Level,Mediator,CommonAncestor):-
%    state_lineage_remove_dup(StateId,Level,Mediator,CommonAncestor),    
%    %StateId<CommonAncestor.
%    state(StateId).

state_lineage_test(StateId,Level,CommonAncestor):-
    state_lineage_remove_dup(StateId,Level,CommonAncestor),    
%    %StateId<CommonAncestor.
    state(StateId).
#show state_lineage_test/3.
EOT
clingo ipaw_2021_demo.openrefine.3.extract/facts.pl rules/column_query.pl  temp_runner.pl
#clingo airbnb_dirty-csv.openrefine-2.extract/facts.pl rules/column_query.pl  temp_runner.pl


echo "What are the sequence of transformation executed on the dataset ?"
cat <<EOT > temp_runner.pl
state(N) :- N=$state.
#show sequence_order/1.
EOT
clingo 03_poster_demo.openrefine.extract/facts.pl rules/column_query.pl  temp_runner.pl

echo

echo "What are the order of row at state $state ?"
cat <<EOT > temp_runner.pl
state(N) :- N=$state.
#show row_order_pos/1.
%#show row_order_pos/4.
%#show row_position_at_state_pair/3.
EOT
clingo 03_poster_demo.openrefine.extract/facts.pl rules/column_query.pl  temp_runner.pl


echo "What are the order and name of the columns at state $state ?"
cat <<EOT > temp_runner.pl
state(N) :- N=$state.
%#show all_column_name_at_state/4.
%#show column_edge/2.
%#show column_edge_at_state/3.
%#show all_column_name_at_state_test/4.
all_column_schema_at_before_state_test(A,B) :-
    all_column_schema_at_before_state(A,B),
    state(B).
all_column_schema_at_state_test(A,B,C) :-
    all_column_schema_at_state(A,B,C),
    state(C).    
%#show all_column_schema_at_before_state_test/2.
%#show all_column_schema_at_state_test/3.
#show column_order/1.
EOT
clingo 03_poster_demo.openrefine.extract/facts.pl rules/column_query.pl  temp_runner.pl

echo "What are the snapshot values at the state $state ?"
cat <<EOT > temp_runner.pl
state(N) :- N=$state.

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

change_count_at_before_state(B) :- content(A,B,N,_,_),
    state(N).

change_count_at_state(B) :- content(A,B,N,_,_),
    state(N).

change_content_at_state_test(ContentId, CellId, ColNum, RowNum, Value, StateId) :-
    change_content_at_state(ContentId, CellId, StateId),
    value(ValueId,Value),
    cell(CellId,ColNum,RowNum),
    content(ContentId,_,_,ValueId,_),
    state(StateId).

%#show all_column_schema_at_before_state_test/2.
%#show all_column_schema_at_state_test/3.
#show change_content_at_state_test/6.
EOT
clingo 03_poster_demo.openrefine.extract/facts.pl rules/column_query.pl  temp_runner.pl

echo "How many cell changes at the state $state ?"
cat <<EOT > temp_runner.pl
state(N) :- N=$state.

content(-1,nul,nul,nul,nul).

change_count_at_state(C) :- C = #count{B: content(_,B,N,_,_)},
    state(N).

#show change_count_at_state/1.
EOT
clingo 03_poster_demo.openrefine.extract/facts.pl rules/column_query.pl  temp_runner.pl



echo "What are the order and name of the columns at state $state ?"
cat <<EOT > temp_runner.pl
state(N) :- N=$state.
%#show all_column_name_at_state/4.
%#show column_edge/2.
%#show column_edge_at_state/3.
%#show all_column_name_at_state_test/4.
all_column_schema_at_before_state_test(A,B) :-
    all_column_schema_at_before_state(A,B),
    state(B).
all_column_schema_at_state_test(A,B,C) :-
    all_column_schema_at_state(A,B,C),
    state(C).    
%#show all_column_schema_at_before_state_test/2.
%#show all_column_schema_at_state_test/3.
#show column_order/1.
EOT
clingo 03_poster_demo.openrefine.extract/facts.pl rules/column_query.pl  temp_runner.pl

echo "What are the column dependency ?"
cat <<EOT > temp_runner.pl
state(N) :- N=$state.
#show column_dependency_order/1.
EOT
clingo 03_poster_demo.openrefine.extract/facts.pl rules/column_query.pl  temp_runner.pl
