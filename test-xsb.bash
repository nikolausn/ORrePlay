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

END_XSB_STDIN