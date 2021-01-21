xsb --quietload --noprompt --nofeedback --nobanner << END_XSB_STDIN
['/Users/nnp2/Documents/projects/openrefine/airbnb_dirty-csv.openrefine.extract/facts.pl'].
['/Users/nnp2/Documents/projects/openrefine/general_rules'].

set_prolog_flag(unknown, fail).
%-------------------------------------------------------------------------------
banner( 'Q1',
        'What are the names of files from which data was imported?',
        'q1(SourceUri)').
[user].
:- table q1/1.
q1(SourceUri) :-
    source(_, SourceUri, _).
end_of_file.
printall(q1(_)).
%-------------------------------------------------------------------------------

%-------------------------------------------------------------------------------
banner( 'Q2',
        '',
        'q2').
[user].
:- table  q2/1.
q2(State) :-
    state(State,_,_).
end_of_file.
printall(q2(_)).
%-------------------------------------------------------------------------------

%-------------------------------------------------------------------------------
demo_banner( 'Demo 1', 'Columns at state?').
[user].
:- table  column_name_at_state/3.
column_name_at_state(Column, State, Name) :-
    column_schema_at_state(Schema, Column, State),
    column_schema(Schema, _, _, _, Name, _, _).
column_rename(State1, Name1, State2, Name2) :-
    column_name_at_state(Column, State1, Name1),
    column_name_at_state(Column, State2, Name2),
    State2 > State1,
    Name2 \== Name1.
d1(Dataset) :-
    import_state(_, Dataset, Array, ImportState),
    final_array_state(Array, FinalState),
    column_rename(ImportState, OriginalName, FinalState, FinalName),
    fmt_write('The column originally named "%S\" was ultimately named "%S".\n', fmt(OriginalName, FinalName)),
    fail
    ;
    true.
end_of_file.

printall(column_name_at_state(_,1,_)).

%-------------------------------------------------------------------------------

END_XSB_STDIN