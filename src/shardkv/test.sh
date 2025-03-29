#!/bin/bash
go test -run=^TestStaticShards$
sleep 3
go test -run=^TestJoinLeave$
sleep 3
go test -run=^TestSnapshot$
sleep 3
go test -run=^TestConcurrent1$
sleep 3
go test -run=^TestConcurrent2$
sleep 10
go test -run=^TestConcurrent3$
sleep 3
go test -run=^TestUnreliable1$
sleep 3
go test -run=^TestUnreliable2$
sleep 3
go test -run=^TestUnreliable3$
sleep 3
go test -run=^TestChallenge1Delete$
sleep 3
go test -run=^TestChallenge2Unaffected$
sleep 3
