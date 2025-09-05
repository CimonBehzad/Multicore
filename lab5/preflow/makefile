DATA	= ../../data

all:
	cargo run < $(DATA)/tiny/0.in | grep '^f = ' | sed 's/f = //' > out
	diff $(DATA)/tiny/0.ans out

	cargo run < $(DATA)/railwayplanning/sample/1.in | grep '^f = ' | sed 's/f = //' > out
	diff $(DATA)/railwayplanning/sample/1.ans out

	cargo run < $(DATA)/railwayplanning/secret/0mini.in | grep '^f = ' | sed 's/f = //' > out
	diff $(DATA)/railwayplanning/secret/0mini.ans out

	cargo run < $(DATA)/railwayplanning/secret/1small.in | grep '^f = ' | sed 's/f = //' > out
	diff $(DATA)/railwayplanning/secret/1small.ans out

	cargo run < $(DATA)/railwayplanning/secret/2med.in | grep '^f = ' | sed 's/f = //' > out
	diff $(DATA)/railwayplanning/secret/2med.ans out

	cargo run < $(DATA)/railwayplanning/secret/3large.in | grep '^f = ' | sed 's/f = //' > out
	diff $(DATA)/railwayplanning/secret/3large.ans out

	cargo run < $(DATA)/railwayplanning/secret/4huge.in | grep '^f = ' | sed 's/f = //' > out
	diff $(DATA)/railwayplanning/secret/4huge.ans out

	cargo run < $(DATA)/big/000.in | grep '^f = ' | sed 's/f = //' > out
	diff $(DATA)/big/000.ans out

	cargo run < $(DATA)/big/001.in | grep '^f = ' | sed 's/f = //' > out
	diff $(DATA)/big/001.ans out

	cargo run < $(DATA)/big/002.in | grep '^f = ' | sed 's/f = //' > out
	diff $(DATA)/big/002.ans out

	cargo run < $(DATA)/huge/001.in | grep '^f = ' | sed 's/f = //' > out
	diff $(DATA)/huge/001.ans out

	@echo PASS all tests
