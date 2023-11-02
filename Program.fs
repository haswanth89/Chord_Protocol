//importing the Chord protocol file.
open Chord

[<EntryPoint>]
let executeMain args =
    match args.Length with
    | 2 ->  //if the length of arguments is 2 as needed
        let numberOfPeers = int args.[0]  //number of peers
        let numberOfRequests = int args.[1] //number of requests 
        connect numberOfPeers numberOfRequests  //triggering the start from chord.fsx
        0
    | _ -> //all other cases 
        printfn "Usage: dotnet run <number_of_peers> <number_of_requests>"
        1 //printing the exception message. 
