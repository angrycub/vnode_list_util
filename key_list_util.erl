%% -------------------------------------------------------------------
%%
%% key_list_util: utility console script for per-vnode key counting, siblings logging and more
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% TODOs
%% * replace foreach loops with foldl loops, taking errors into account and returning them

-module(key_list_util).
-compile(export_all).
-compile([{parse_transform, lager_transform}]).

% Describes the Contents of a Riak object. A "sibling" is an instance of this record.
% Duplicated from riak_kv/riak_object, since it's needed by compare_content_dates()
-record(r_content, {
          metadata :: dict(),
          value :: term()
         }).

%% =================================================================================================
%% In the following functions, Bucket can be a Bucket or a {BucketType, Bucket} pair.
%% Including this parameter implies the operation will be run only upon the specified Bucket or pair.

count_all_keys(OutputDir) ->
    process_cluster_parallel(OutputDir, [{output_dir,OutputDir}, count_keys]).

count_all_keys_for_bucket(OutputDir, Bucket) ->
    process_cluster_parallel(OutputDir, [{output_dir,OutputDir}, count_keys, {bucket_name, Bucket}]).

log_all_keys(OutputDir) ->
    process_cluster_parallel(OutputDir, [{output_dir,OutputDir}, log_keys]).

log_all_keys_for_bucket(OutputDir, Bucket) ->
    process_cluster_parallel(OutputDir, [{output_dir,OutputDir}, log_keys, {bucket_name, Bucket}]).

% SleepPeriod - optional amount of time to sleep between each key operation,
% in milliseconds
log_all_keys(OutputDir, SleepPeriod) ->
    process_cluster_parallel(OutputDir, [{output_dir,OutputDir}, log_keys, {sleep_for, SleepPeriod}]).

log_all_keys_for_bucket(OutputDir, Bucket, SleepPeriod) ->
    process_cluster_parallel(OutputDir, [{output_dir,OutputDir}, log_keys, {sleep_for, SleepPeriod}, {bucket_name, Bucket}]).

resolve_all_siblings(OutputDir) ->
    process_cluster_serial(OutputDir, [{output_dir,OutputDir}, log_siblings, resolve_siblings]).

resolve_all_siblings_for_bucket(OutputDir, Bucket) ->
    process_cluster_serial(OutputDir, [{output_dir,OutputDir},log_siblings, resolve_siblings, {bucket_name, Bucket}]).

local_direct_delete(Index, Bucket, Key) when
    is_integer(Index),
    is_binary(Bucket),
    is_binary(Key) ->
    DeleteReq = {riak_kv_delete_req_v1, {Bucket, Key}, make_ref()},
    riak_core_vnode_master:sync_command({Index, node()}, DeleteReq, riak_kv_vnode_master).

get_preflist_for_key(Bucket, Key, NValue) when 
    is_binary(Bucket),
    is_binary(Key),
    is_integer(NValue) ->
    BKey = {Bucket,Key},
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    DocIdx = riak_core_util:chash_key(BKey),
    % BucketProps = riak_core_bucket:get_bucket(Bucket, Ring), 
    % [NValue] = [Y || {X1, Y} <- BucketProps, n_val == X1],
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    Preflist = riak_core_apl:get_apl_ann(DocIdx, NValue, Ring, UpNodes),
    [IndexNode || {IndexNode, _Type} <- Preflist].


%% =================================================================================================


% Used for sorting an object's siblings in modified timestamp order (most recently modified to least)
% Duplicated from riak_kv/riak_object (since it's not exported from that module)
compare_content_dates(C1, C2) ->
    D1 = dict:fetch(<<"X-Riak-Last-Modified">>, C1#r_content.metadata),
    D2 = dict:fetch(<<"X-Riak-Last-Modified">>, C2#r_content.metadata),
    %% true if C1 was modifed later than C2
    Cmp1 = riak_core_util:compare_dates(D1, D2),
    %% true if C2 was modifed later than C1
    Cmp2 = riak_core_util:compare_dates(D2, D1),
    %% check for deleted objects
    Del1 = dict:is_key(<<"X-Riak-Deleted">>, C1#r_content.metadata),
    Del2 = dict:is_key(<<"X-Riak-Deleted">>, C2#r_content.metadata),

    SameDate = (Cmp1 =:= Cmp2),
    case {SameDate, Del1, Del2} of
        {false, _, _} ->
            Cmp1;
        {true, true, false} ->
            false;
        {true, false, true} ->
            true;
        _ ->
            %% Dates equal and either both present or both deleted, compare
            %% by opaque contents.
            C1 < C2
    end.

get_vtag(Obj) ->
    dict:fetch(<<"X-Riak-VTag">>, Obj#r_content.metadata).

is_deleted(Obj) ->
    dict:is_key(<<"X-Riak-Deleted">>, Obj#r_content.metadata).

% Loads the contents of a module (this module, usually) on every node in the cluster,
% to parallelize and cut down on inter-node disterl chatter.
load_module_on_nodes(Module, Nodes) ->
    rpc:multicall(Nodes, code, purge, [Module]),
    case code:get_object_code(Module) of
        {Module, Bin, File} ->
            case rpc:multicall(Nodes, code, load_binary, [Module, File, Bin]) of
                {_, []} -> ok;
                {_, Failures} ->
                    throw(lists:flatten(io_lib:format("Unable to reach the following nodes: ~p", [Failures])))
            end;
        error ->
            error(lists:flatten(io_lib:format("unable to get_object_code(~s)", [Module])))
    end.

% Log the vtag, value and deleted status of a given sibling object
log_sibling_contents(Obj, OutputFilename) ->
    DateModified = calendar:now_to_local_time(dict:fetch(<<"X-Riak-Last-Modified">>, Obj#r_content.metadata)),
    Deleted = is_deleted(Obj),
    Vtag = get_vtag(Obj),
    Msg = io_lib:format("~p~n", [{{vtag, Vtag}, {date_modified, DateModified}, {is_deleted, Deleted}}]),
    file:write_file(OutputFilename, Msg, [append]),
    Value = Obj#r_content.value,
    file:write_file(OutputFilename, io_lib:format("~p~n", [Value]), [append]).

% Returns the last (most recent) version of the object (by timestamp)
% If the last version is a tombstone, return the
% (The list of siblings is pre-sorted by timestamp, most recent to least)
last_valid_vtag([]) ->
    {error, "No valid (non-deleted) sibling found."};
last_valid_vtag([Sibling|Rest]) ->
    case is_deleted(Sibling) of
        false ->
            {ok, {get_vtag(Sibling), Sibling}};
            % {ok, {test, test}};
        true ->
            last_valid_vtag(Rest)
    end.

resolve_object_siblings(OutputFilename, Bucket, Key, SiblingsByDate) ->
    case last_valid_vtag(SiblingsByDate) of
        {ok, {CorrectVtag, CorrectSibling}} ->
            case force_reconcile(Bucket, Key, CorrectSibling) of
                ok ->
                    Msg = io_lib:format("Resolved to Vtag: ~p~n", [CorrectVtag]);
                {error, Error} ->
                    Msg = io_lib:format("Error resolving to Vtag ~p :: ~p~n", [CorrectVtag, Error])
            end;
        {error, Error} ->
            Msg = io_lib:format("Error resolving siblings: ~p~n", [{Error}])
    end,
    file:write_file(OutputFilename, Msg, [append]).

force_reconcile(Bucket, Key, CorrectSibling) ->
    {ok, C} = riak:local_client(),
    {ok, OldObj} = C:get(Bucket, Key),
    NewObj = riak_object:update_metadata(riak_object:update_value(OldObj, CorrectSibling#r_content.value), CorrectSibling#r_content.metadata),
    UpdatedObj = riak_object:apply_updates(NewObj),
    C:put(UpdatedObj, all, all).  % W=all, DW=all

% Convert a serialized binary into a riak_object() record
% The function riak_object:from_binary() was introduced in 1.4, so
% we need to check for its existence and use it if possible
unserialize(_B, _K, Val) when is_tuple(Val), element(1,Val) =:= r_object->
    Val;
unserialize(_B, _K, <<131, _Rest/binary>>=Val) ->
    binary_to_term(Val);
unserialize(Bucket, Key, Val) ->
    try
        riak_object:from_binary(Bucket, Key, Val)
    catch _:_ ->
        {error, bad_object_format}
    end.

% Log all siblings for a riak object (if any exist).
log_or_resolve_siblings(OutputFilename, Bucket, Key, ObjBinary, Options) ->
    Obj = unserialize(Bucket, Key, ObjBinary),
    SiblingCount = case Obj of
        {error, _Error} ->
            1;  % Error unserializing, skip the logging of the sibling count, below
        _ ->
            riak_object:value_count(Obj)
    end,

    if SiblingCount > 1 ->
        Contents = riak_object:get_contents(Obj),
        SiblingsByDate = lists:sort(fun compare_content_dates/2, Contents),
        Msg = io_lib:format("~n~p~n", [{Bucket, Key, SiblingCount}]),
        file:write_file(OutputFilename, Msg, [append]),

        lists:foreach(fun(Sibling) -> log_sibling_contents(Sibling, OutputFilename) end, SiblingsByDate),

        case lists:member(resolve_siblings, Options) of
            true ->
                resolve_object_siblings(OutputFilename, Bucket, Key, SiblingsByDate);
            _ -> ok
        end;
    true -> ok
    end.

member_nodes() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:all_members(Ring).

% For each node in the cluster, in parallel, load this module,
% and invoke the process_node() function on its vnodes.
process_cluster_parallel(OutputDir, Options) ->
    io:format("Scanning all nodes in parallel...~n"),
    Members = member_nodes(),
    load_module_on_nodes(?MODULE, Members),
    {Results, Failures} = riak_core_util:rpc_every_member_ann(?MODULE, process_node, [OutputDir, Options], 86400000),
%%    io:format("{Results, Failures} = {~p,~p}",[Results, Failures]),
    case Failures of 
        [] ->
            FoldFun = fun({_Node, ResultDict}, Accumulator) when is_tuple(ResultDict), element(1,ResultDict) =:= dict ->
                case dict:is_key(<<"VnodeKeyCounts">>, ResultDict) of
                    true ->
                        TotalClusterCountDict = case dict:is_key(<<"VnodeKeyCounts">>, Accumulator) of
                            true -> dict:fetch(<<"VnodeKeyCounts">>, Accumulator);
                            false -> dict:new()
                        end,

                        Counts = dict:to_list(dict:fetch(<<"VnodeKeyCounts">>, ResultDict)),
                        WriteBucketFun = fun(BucketCount, VnodeKeyCountAcc) ->
                            {Bucket, Count} = BucketCount,
                            dict:update_counter(Bucket, Count, VnodeKeyCountAcc)
                        end,
                        NewVnodeKeyCountDict = lists:foldl(WriteBucketFun, TotalClusterCountDict, Counts), 
                        dict:store(<<"VnodeKeyCounts">>, NewVnodeKeyCountDict, Accumulator);
                    _-> 
                        Accumulator
                end;
            ({Node, BrokeDict}, _Acc) ->
                io:format("~p~n",[BrokeDict]),
                lager:error("~s didn't return a dict. ~p",[Node, BrokeDict]),
                throw(lists:flatten(io_lib:format("~s didn't return a dict.", [Node])))
            end,

            ClusterCountsFilename = filename:join(OutputDir, [io_lib:format("cluster-counts.log", [])]),
            ClusterResults = lists:foldl(FoldFun, dict:new(), Results),
            write_node_totals(ClusterCountsFilename, ClusterResults);

        Failures ->
            lager:warning("Skipping cluster totals--One or more nodes failed to respond. [~p]",[Failures])
    end,

    io:format("Done.~n", []).

% For each node in the cluster, load this module,
% and invoke the process_node() function on its vnodes.
process_cluster_serial(OutputDir, Options) ->
    io:format("Scanning all nodes serially...~n"),
    Members = member_nodes(),
    load_module_on_nodes(?MODULE, Members),
    NodeFun = fun(Node) ->
        io:format("Processing node ~p~n", [Node]),
        rpc:call(Node, ?MODULE, process_node, [OutputDir, Options])
    end,
    lists:foreach(NodeFun, Members),
    io:format("Done.~n").


% Invoked on each member node in the ring
% Calls process_vnode() on each vnode local to this node.
process_node(OutputDir, Options) ->
    SelectedVnodes  = case proplists:get_value(vnodes,Options,primaries) of
        primaries -> 
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            Owners = riak_core_ring:all_owners(Ring),
            [Index || {Index, Owner} <- Owners, Owner =:= node()];
        running ->
            [Index || {riak_kv_vnode,Index,_Owner} <- riak_core_vnode_manager:all_vnodes()];
        fallbacks ->
            {ok,Ring} = riak_core_ring_manager:get_my_ring(),
            Locals = [{Idx,Pid,riak_core_ring:index_owner(Ring,Idx)} || {Idx,Pid} <- riak_core_vnode_manager:all_index_pid(riak_kv_vnode)],
            [Index || {Index,_Pid,Owner} <- Locals, Owner =/= node()];
        all ->
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            Owners = riak_core_ring:all_owners(Ring),
            [Index || {Index, _Owner} <- Owners];
        VnodeList when is_list(VnodeList) ->
            CleanItems = [Index || Index <- VnodeList, is_integer(Index) ],
            if 
                length(CleanItems) =/= length(VnodeList) -> 
                    lager:warning("VnodeList contains non-integer items",[]);
                true -> ok
            end,
            CleanItems;
        Option ->
            lager:error("Invalid `vnodes` option. [~p]",[Option])
    end,
    lager:info("SelectedVnodes = ~p.",[length(SelectedVnodes)]),
    NodeResults = lists:foldl(fun(Vnode, NodeResultAccumulator) -> process_vnode(Vnode, OutputDir, Options, NodeResultAccumulator) end, dict:new(), SelectedVnodes),
    NodeCountsFilename = filename:join(OutputDir, [io_lib:format("~s-counts.log", [node()])]),
    write_node_totals(NodeCountsFilename, NodeResults),
    NodeResults.

% Performs a riak_kv_vnode:fold(), and invokes logging functions for each key in this partition
process_vnode(Vnode, OutputDir, Options, NodeResultAccumulator) ->
    Node = node(),

    CountsFilename = filename:join(OutputDir, [io_lib:format("~s-~p-counts.log", [Node, Vnode])]),

    {InitialAccumulator,ProcessObj} = processObj(Options, Vnode),

    lager:info("Begin processing partition ~p, with options: ~p",[Vnode, Options]),

    Results = riak_kv_vnode:fold({Vnode, Node}, ProcessObj, InitialAccumulator),
    NewNodeResultAccumulator = write_vnode_totals(CountsFilename, Results, NodeResultAccumulator),

    lager:info("Complete processing partition ~p",[Vnode]),
    NewNodeResultAccumulator.

write_vnode_totals(OutputFilename, Results, NodeResultAccumulator) ->
    case dict:is_key(<<"BucketKeyCounts">>, Results) of
        true ->
            VnodeKeyCountDict = case dict:is_key(<<"VnodeKeyCounts">>, NodeResultAccumulator) of
                true -> dict:fetch(<<"VnodeKeyCounts">>, NodeResultAccumulator);
                false -> dict:new()
            end,

            {ok, File} = file:open(OutputFilename, [write]),
            Counts = dict:to_list(dict:fetch(<<"BucketKeyCounts">>, Results)),
            WriteBucketFun = fun(BucketCount, VnodeKeyCountAcc) ->
                {Bucket, Count} = BucketCount,
                io:format(File,"{~p,~B}.~n", [Bucket, Count]),
                dict:update_counter(Bucket, Count, VnodeKeyCountAcc)
            end,
            NewVnodeKeyCountDict = lists:foldl(WriteBucketFun, VnodeKeyCountDict,Counts), %% TODO foreach
            file:close(File),
            dict:store(<<"VnodeKeyCounts">>, NewVnodeKeyCountDict, NodeResultAccumulator);

        _ -> NodeResultAccumulator %% TODO return a result
    end.

write_node_totals(OutputFilename, Results) ->
    case dict:is_key(<<"VnodeKeyCounts">>, Results) of
        true ->
            {ok, File} = file:open(OutputFilename, [write]),
            Counts = dict:to_list(dict:fetch(<<"VnodeKeyCounts">>, Results)),
            WriteBucketFun = fun(BucketCount) ->
                {Bucket, Count} = BucketCount,
                io:format(File,"{~p,~B}.~n", [Bucket, Count])
            end,
            lists:foreach(WriteBucketFun, Counts),
            file:close(File);
        _ -> ok 
    end.

processOptions(Options, Vnode, Acc) ->
    OutputDir = proplists:get_value(output_dir, Options, "/var/tmp"),
    SleepFor = proplists:get_value(sleep_for, Options, undefined),
    InitialAccumulator = dict:new(),    
    processOptions(Options, Vnode, OutputDir, SleepFor, InitialAccumulator, Acc).

processOptions([], _Vnode, _OutputDir, _SleepFor, InitialAccumulator, {Uses, Funs}) ->
    {InitialAccumulator, Uses, Funs};

processOptions([Option|Rest], Vnode, OutputDir, SleepFor, InitialAccumulator, {Uses, Funs} = Acc) ->
    Node = node(), 

    case Option of
        log_keys ->
            KeysFilename = filename:join(OutputDir, [io_lib:format("~s-~p-keys.log", [Node, Vnode])]),
            {ok, File} = file:open(KeysFilename, [write]),
            ProcessFun = case SleepFor of
                SleepPeriod when is_integer(SleepPeriod)->
                    fun(Bucket, Key, _Object, _ObjBinary, AccDict) ->
                        io:format(File,"~p,~s~n", [Bucket, binary_to_list(Key)]),
                        timer:sleep(SleepPeriod),
                        AccDict
                    end;

                undefined -> 
                    fun(Bucket, Key, _Object, _ObjBinary, AccDict) ->
                        io:format(File,"~p,~s~n", [Bucket, binary_to_list(Key)]),
                        AccDict
                    end;

                Stupid ->
                    throw(io_lib:format("Bad value provided for `sleep_for` - [~p]",[Stupid]))
                end,            
            processOptions(Rest, Vnode, OutputDir, SleepFor, InitialAccumulator, {Uses, [ProcessFun | Funs]});

        count_keys ->
            ProcessFun = fun(Bucket, _Key, _Object, _ObjBinary, AccDict) ->
                CountDict = dict:update_counter(Bucket, 1, dict:fetch(<<"BucketKeyCounts">>, AccDict)),
                dict:store(<<"BucketKeyCounts">>, CountDict, AccDict)
            end,
            NewAccumulator = dict:store(<<"BucketKeyCounts">>, dict:new(), InitialAccumulator),
            processOptions(Rest, Vnode, OutputDir, SleepFor, NewAccumulator, {Uses, [ProcessFun | Funs]});

        log_siblings ->
            error("log_siblings is not implemented yet",[]);

        _ -> processOptions(Rest, Vnode, OutputDir, SleepFor, InitialAccumulator, Acc)
    end.        

processObj(Options, Vnode) ->
    {ProcessAccumulator, ProcessesUseObjects, FoldFuns} = processOptions(Options, Vnode, {false,[]}),
    {InitialAccumulator, FiltersUseObjects, FilterFuns} = processFilters(Options, Vnode, {ProcessAccumulator,false,[]}),
    UseObjects = ProcessesUseObjects or FiltersUseObjects,

    ProcessFun = fun({Bucket, Key}, ObjBinary, AccDict) ->
        {BucketType, BucketName} = case is_tuple(Bucket) of 
            true ->
                Bucket;
            _ ->
                {none, Bucket}
        end,
        Object = case UseObjects of 
            true -> unserialize(Bucket, Key, ObjBinary);
            false -> not_unserialized
        end,
        case lists:dropwhile(fun(X) -> X(BucketType, BucketName, Key, Object, ObjBinary) end, FilterFuns) of 
            [] -> lists:foldl(fun(X, Acc) -> X(Bucket, Key, Object, ObjBinary, Acc) end, AccDict, FoldFuns);
            _ -> AccDict   %% this just means that one of the filters evaluated to false and we shouldn't try to deal with it
        end
    end,
    {InitialAccumulator, ProcessFun}.

processFilters([], _Vnode, {ProcessAccumulator, Uses, Funs}) ->
    {ProcessAccumulator, Uses, lists:reverse(Funs)};

processFilters([Option|Rest], Vnode, {ProcessAccumulator, Uses, Funs} = Acc) ->
    case Option of
        {bucket_type, FilteredBucketType} ->
            FilterFun = fun(BucketType, _BucketName, _Key, _Object, _Binary) ->
                FilteredBucketType =:= BucketType
            end,
            processFilters(Rest, Vnode, {ProcessAccumulator, Uses, [FilterFun | Funs]});

        {bucket_name, FilteredBucketName} ->
            FilterFun = fun(_BucketType, BucketName, _Key, _Object, _Binary) ->
                FilteredBucketName =:= BucketName
            end,
            processFilters(Rest, Vnode, {ProcessAccumulator, Uses, [FilterFun | Funs]});
        
        {bucket_name_prefix, BucketPrefix} ->
            FilterFun = fun(_BucketType, BucketName, _Key, _Object, _Binary) ->
                isBinaryPrefix(BucketPrefix, BucketName)
            end,
            processFilters(Rest, Vnode, {ProcessAccumulator, Uses, [FilterFun | Funs]});

        {key_prefix, KeyPrefix} ->
            FilterFun = fun(_BucketType, _BucketName, Key, _Object, _Binary) ->
                isBinaryPrefix(KeyPrefix, Key)
            end,
            processFilters(Rest, Vnode, {ProcessAccumulator, Uses, [FilterFun | Funs]});

        _ -> 
            processFilters(Rest, Vnode, Acc)
    end.        

isBinaryPrefix(PrefixCandidate, Value) when is_binary(PrefixCandidate), is_binary(Value) ->
    PrefixSize = byte_size(PrefixCandidate),
    try 
        <<PrefixCandidate:PrefixSize/binary, _Rest/binary>> = Value,
        true
    catch 
        _:_ -> false
    end;

isBinaryPrefix(_, _) -> false.

