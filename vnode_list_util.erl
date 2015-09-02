%% -------------------------------------------------------------------
%%
%% vnode_list_util: utility console script for per-vnode key counting, 
%%                  siblings logging and more
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
%% * once upon a time

-module(vnode_list_util).
-export([
    count_everything/1,
    count_all_keys/1,
    count_all_keys_for_bucket/2,
    log_all_keys/1,
    log_all_keys/2,
    log_all_keys_for_bucket/2,
    log_all_keys_for_bucket/3,
    resolve_all_siblings/1,
    resolve_all_siblings_for_bucket/2
    ]).

-export([process_cluster_parallel/2, process_node/2]).

-export([build_test_data/0, build_time_test_data/6, clean_cluster/0, vnode_get/2]).

-compile([{parse_transform, lager_transform}]).

% Describes the Contents of a Riak object. A "sibling" is an instance of this record.
% Duplicated from riak_kv/riak_object, since it's needed by compare_content_dates()
-record(r_content, {
          metadata :: dict(),
          value :: term()
         }).


%% #################################################################################################
%% #                 ###############################################################################
%% # Public API funs ###############################################################################
%% #                 ###############################################################################
%% #################################################################################################

%% =================================================================================================
%% In the following functions, Bucket can be a Bucket or a {BucketType, Bucket} pair.
%% Including this parameter implies the operation will be run only upon the specified Bucket 
%% or pair.

count_everything(OutputDir) ->
    process_cluster_parallel(OutputDir, 
        [{output_dir,OutputDir}, count_keys, {vnodes,all}]).

count_all_keys(OutputDir) ->
    process_cluster_parallel(OutputDir, 
        [{output_dir,OutputDir}, count_keys]).

count_all_keys_for_bucket(OutputDir, Bucket) ->
    process_cluster_parallel(OutputDir, 
        [{output_dir,OutputDir}, count_keys, {bucket_name, Bucket}]).

log_all_keys(OutputDir) ->
    process_cluster_parallel(OutputDir, 
        [{output_dir,OutputDir}, log_keys]).

log_all_keys(OutputDir, SleepPeriod) -> %% SleepPeriod is in milliseconds.
    process_cluster_parallel(OutputDir, 
        [{output_dir,OutputDir}, log_keys, {sleep_for, SleepPeriod}]).

log_all_keys_for_bucket(OutputDir, Bucket) ->
    process_cluster_parallel(OutputDir, 
        [{output_dir,OutputDir}, log_keys, {bucket_name, Bucket}]).

log_all_keys_for_bucket(OutputDir, Bucket, SleepPeriod) -> %% SleepPeriod is in milliseconds.
    process_cluster_parallel(OutputDir, 
        [{output_dir,OutputDir}, log_keys, {sleep_for, SleepPeriod}, {bucket_name, Bucket}]).

resolve_all_siblings(OutputDir) ->
    process_cluster_serial(OutputDir, 
        [{output_dir,OutputDir}, log_siblings, resolve_siblings]).

resolve_all_siblings_for_bucket(OutputDir, Bucket) ->
    process_cluster_serial(OutputDir, 
        [{output_dir,OutputDir},log_siblings, resolve_siblings, {bucket_name, Bucket}]).








%% #################################################################################################
%% #                 ###############################################################################
%% # Processing funs ###############################################################################
%% #                 ###############################################################################
%% #################################################################################################

%% =================================================================================================
%% For each node in the cluster, in parallel, load this module,
%% and invoke the process_node() function on its vnodes.

process_cluster_parallel(OutputDir, Options) ->
    io:format("Scanning all nodes in parallel...~n"),
    Members = member_nodes(),
    load_module_on_nodes(?MODULE, Members),
    {Results, Failures} = riak_core_util:rpc_every_member_ann(
        ?MODULE, process_node, [OutputDir, Options], timer:hours(24)),
%%    io:format("{Results, Failures} = {~p,~p}",[Results, Failures]),
    case Failures of 
        [] ->
            FoldFun = fun({_Node, ResultDict}, Accumulator) when 
                is_tuple(ResultDict), 
                element(1,ResultDict) =:= dict ->
                    dict:merge(fun dict_merge_fun/3, ResultDict, Accumulator);
            ({Node, BrokeDict}, _Acc) ->
                io:format("~p~n",[BrokeDict]),
                lager:error("~s didn't return a tagged orddict. ~p",[Node, BrokeDict]),
                throw(lists:flatten(io_lib:format("~s didn't return a tagged orddict.", [Node])))
            end,

            ClusterCountsFilename = filename:join(OutputDir, 
                [io_lib:format("cluster-counts.log", [])]),
            ClusterResults = lists:foldl(FoldFun, dict:new(), Results),
            write_node_totals(ClusterCountsFilename, ClusterResults),
            dict_pretty_print(ClusterResults);
        Failures ->
            lager:warning(
                "Skipping cluster totals--One or more nodes failed to respond. [~p]",[Failures])
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
    SelectedVNodes  = case proplists:get_value(vnodes,Options,primaries) of
        primaries -> 
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            Owners = riak_core_ring:all_owners(Ring),
            [Index || {Index, Owner} <- Owners, Owner =:= node()];
        running ->
            [Index || {riak_kv_vnode,Index,_Owner} <- riak_core_vnode_manager:all_vnodes()];
        fallbacks ->
            {ok,Ring} = riak_core_ring_manager:get_my_ring(),
            Locals = lists:foldl(
                fun({Idx, Pid}, Acc) -> 
                    [{Idx, Pid, riak_core_ring:index_owner(Ring, Idx)}|Acc]
                end, 
                [],
                riak_core_vnode_manager:all_index_pid(riak_kv_vnode)),
            [Index || {Index,_Pid,Owner} <- Locals, Owner =/= node()];
        all ->
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            Owners = riak_core_ring:all_owners(Ring),
            [Index || {Index, _Owner} <- Owners];
        VNodeList when is_list(VNodeList) ->
            CleanItems = [Index || Index <- VNodeList, is_integer(Index) ],
            if 
                length(CleanItems) =/= length(VNodeList) -> 
                    lager:warning("VNodeList contains non-integer items",[]);
                true -> ok
            end,
            CleanItems;
        Option ->
            lager:error("Invalid `vnodes` option. [~p]",[Option])
    end,
    lager:info("SelectedVNodes = ~p.",[length(SelectedVNodes)]),
    NodeResults = lists:foldl(
        fun(VNode, NodeResultAccumulator) -> 
            process_vnode(VNode, OutputDir, Options, NodeResultAccumulator) 
        end, dict:new(), SelectedVNodes),
    NodeCountsFilename = filename:join(OutputDir, [io_lib:format("~s-counts.log", [node()])]),
    write_node_totals(NodeCountsFilename, NodeResults),
    NodeResults.

% Performs a riak_kv_vnode:fold(), and invokes logging functions for each key in this partition
process_vnode(VNode, OutputDir, Options, NodeResultAccumulator) ->
    Node = node(),

    CountsFilename = filename:join(OutputDir, [io_lib:format("~s-~p-counts.log", [Node, VNode])]),

    {InitialAccumulator,ProcessObj} = processObj(Options, VNode),

    lager:info("Begin processing partition ~p, with options: ~p",[VNode, Options]),

    Results = riak_kv_vnode:fold({VNode, Node}, ProcessObj, InitialAccumulator),
    NewNodeResultAccumulator = write_vnode_totals(CountsFilename, Results, NodeResultAccumulator),

    lager:info("Complete processing partition ~p",[VNode]),
    NewNodeResultAccumulator.

write_vnode_totals(OutputFilename, Results, NodeResultAccumulator) ->
    lager:info("~nResults:~n~s~nNodeResultAccumulator:~n~s~n",
        [dict_pretty_print(Results), dict_pretty_print(NodeResultAccumulator)]), 
    NewNodeResultAccumulator = dict:merge(fun dict_merge_fun/3, Results, NodeResultAccumulator),
    case dict:is_key(<<"BucketKeyCounts">>, Results) of
        true ->
            VNodeKeyCountDict = case dict:is_key(<<"VNodeKeyCounts">>, NodeResultAccumulator) of
                true -> dict:fetch(<<"VNodeKeyCounts">>, NodeResultAccumulator);
                false -> dict:new()
            end,

            {ok, File} = file:open(OutputFilename, [write]),
            Counts = dict:to_list(dict:fetch(<<"BucketKeyCounts">>, Results)),
            WriteBucketFun = fun(BucketCount, VNodeKeyCountAcc) ->
                {Bucket, Count} = BucketCount,
                io:format(File,"{~p,~B}.~n", [Bucket, Count]),
                dict:update_counter(Bucket, Count, VNodeKeyCountAcc)
            end,
            NewVNodeKeyCountDict = lists:foldl(WriteBucketFun, VNodeKeyCountDict, Counts), 
            file:close(File),
            NewNodeResultAccumulator;

        _ -> NewNodeResultAccumulator %% TODO return a result
    end.

write_node_totals(OutputFilename, Results) ->
    case dict:is_key(<<"VNodeKeyCounts">>, Results) of
        true ->
            {ok, File} = file:open(OutputFilename, [write]),
            Counts = dict:to_list(dict:fetch(<<"VNodeKeyCounts">>, Results)),
            WriteBucketFun = fun(BucketCount) ->
                {Bucket, Count} = BucketCount,
                io:format(File,"{~p,~B}.~n", [Bucket, Count])
            end,
            lists:foreach(WriteBucketFun, Counts),
            file:close(File);
        _ -> ok 
    end.








%% #################################################################################################
%% #                        ########################################################################
%% # Object Processing funs ########################################################################
%% #                        ########################################################################
%% #################################################################################################

processOptions(Options, VNode, Acc) ->
    OutputDir = proplists:get_value(output_dir, Options, "/var/tmp"),
    SleepFor = proplists:get_value(sleep_for, Options, undefined),
    InitialAccumulator = dict:new(),    
    processOptions(Options, VNode, OutputDir, SleepFor, InitialAccumulator, Acc).

processOptions([], _VNode, _OutputDir, _SleepFor, InitialAccumulator, {Uses, Funs}) ->
    {InitialAccumulator, Uses, Funs};

processOptions([Option|Rest], VNode, OutputDir, SleepFor, InitialAccumulator, {Uses, Funs} = Acc) ->
    Node = node(), 

    case Option of
        log_keys ->
            KeysFilename = filename:join(
                OutputDir, [io_lib:format("~s-~p-keys.log", [Node, VNode])]),
            {ok, File} = file:open(KeysFilename, [write]),
            ProcessFun = case SleepFor of
                SleepPeriod when is_integer(SleepPeriod)->
                    fun(Bucket, Key, _Object, _ObjBinary, AccDict) ->
                        io:format(File,"{~p,~p}.~n", [Bucket, Key]),
                        timer:sleep(SleepPeriod),
                        AccDict
                    end;

                undefined -> 
                    fun(Bucket, Key, _Object, _ObjBinary, AccDict) ->
                        io:format(File,"{~p,~p}.~n", [Bucket, Key]),
                        AccDict
                    end;

                Stupid ->
                    throw(io_lib:format("Bad value provided for `sleep_for` - [~p]",[Stupid]))
                end,            
            processOptions(Rest, VNode, OutputDir, SleepFor, InitialAccumulator, 
                {Uses, [ProcessFun | Funs]});

        count_keys ->
            ProcessFun = fun(Bucket, _Key, _Object, _ObjBinary, ProcessAccDict) ->
                CountDict = dict:update_counter(Bucket, 1, 
                    dict:fetch(<<"BucketKeyCounts">>, ProcessAccDict)),
                dict:store(<<"BucketKeyCounts">>, CountDict, ProcessAccDict)
            end,
            NewAccumulator = dict:store(<<"BucketKeyCounts">>, dict:new(), InitialAccumulator),
            processOptions(Rest, VNode, OutputDir, SleepFor, NewAccumulator, 
                {Uses, [ProcessFun | Funs]});

        delete_object ->
            ProcessFun = fun(Bucket, Key, _Object, _ObjBinary, ProcessAccDict) ->
                {ok, Client} = riak:local_client(),
                case Client:delete(Bucket, Key) of 
                    ok ->
                        DeleteCountDict = dict:update_counter(success, 1, 
                            dict:fetch(<<"DeleteResultCounts">>, ProcessAccDict));
                    _ -> 
                        DeleteCountDict = dict:update_counter(error, 1, 
                            dict:fetch(<<"DeleteResultCounts">>, ProcessAccDict))
                end,
                dict:store(<<"DeleteResultCounts">>, DeleteCountDict, ProcessAccDict)
            end,
            NewAccumulator = dict:store(<<"DeleteResultCounts">>, dict:new(), InitialAccumulator),
            processOptions(Rest, VNode, OutputDir, SleepFor, NewAccumulator, 
                {Uses, [ProcessFun | Funs]});

        {copy_object, DestinationBucket} ->
            ProcessFun = fun(_Bucket, Key, Object, _ObjBinary, ProcessAccDict) ->
                {ok, Client} = riak:local_client(),
                case Client:put(
                    riak_object:new(
                        DestinationBucket, Key, riak_object:get_values(Object), 
                        riak_object:get_metadatas(Object))) of 
                    ok ->  
                        CopyCountDict = dict:update_counter(success, 1, 
                            dict:fetch(<<"CopyResultCounts">>, ProcessAccDict));
                    _ -> 
                        CopyCountDict = dict:update_counter(false, 1, 
                            dict:fetch(<<"CopyResultCounts">>, ProcessAccDict))
                end,
                dict:store(<<"CopyResultCounts">>, CopyCountDict, ProcessAccDict)
            end,
            NewAccumulator = dict:store(<<"CopyResultCounts">>, dict:new(), InitialAccumulator),
            processOptions(Rest, VNode, OutputDir, SleepFor, NewAccumulator, 
                {true, [ProcessFun | Funs]});

       direct_delete_replica ->
            ProcessFun = fun(Bucket, Key, Object, _ObjBinary, ProcessAccDict) ->
                send_delete_message_to_vnode(VNode, {Bucket, Key}, Object),
                DirectDeleteCountDict = dict:update_counter(delete_requested, 1, 
                    dict:fetch(<<"DirectDeleteResultCounts">>, ProcessAccDict)),
                dict:store(<<"DirectDeleteResultCounts">>, DirectDeleteCountDict, ProcessAccDict)
            end,
            NewAccumulator = dict:store(<<"DirectDeleteResultCounts">>, 
                dict:new(), InitialAccumulator),
            processOptions(Rest, VNode, OutputDir, SleepFor, NewAccumulator, 
                {true, [ProcessFun | Funs]});
            
        log_siblings ->
            error("log_siblings is not implemented yet",[]);
        
        {custom_processing_fun, ProcessFun, NeedsUnserializedObject, AccumulatorKey} when 
        is_function(ProcessFun, 5), is_boolean(NeedsUnserializedObject) ->
            NewAccumulator = dict:store(AccumulatorKey, dict:new(), InitialAccumulator),
            case NeedsUnserializedObject of
                true ->
                    processOptions(Rest, VNode, OutputDir, SleepFor, NewAccumulator, 
                        {true, [ProcessFun | Funs]});
                false ->
                    processOptions(Rest, VNode, OutputDir, SleepFor, NewAccumulator, 
                        {Uses, [ProcessFun | Funs]});
                _ ->
                    erlang:error(badarg)
            end;
        _ -> 
            processOptions(Rest, VNode, OutputDir, SleepFor, InitialAccumulator, Acc)
    end.        

processObj(Options, VNode) ->
    {ProcessAccumulator, ProcessesUseObjects, FoldFuns} = 
        processOptions(Options, VNode, {false,[]}),
    {InitialAccumulator, FiltersUseObjects, FilterFuns} = 
        processFilters(Options, VNode, {ProcessAccumulator,false,[]}),

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
        case lists:dropwhile(
            fun(X) -> X(BucketType, BucketName, Key, Object, ObjBinary) end, FilterFuns) of 
            [] -> lists:foldl(
                fun(X, Acc) -> X(Bucket, Key, Object, ObjBinary, Acc) end, AccDict, FoldFuns);
            _ -> AccDict   %% One of the filters evaluated to false so skip this object
        end
    end,
    {InitialAccumulator, ProcessFun}.







%% #################################################################################################
%% #             ###################################################################################
%% # Filter funs ###################################################################################
%% #             ###################################################################################
%% #################################################################################################

processFilters([], _VNode, {ProcessAccumulator, Uses, Funs}) ->
    {ProcessAccumulator, Uses, lists:reverse(Funs)};

processFilters([Option|Rest], VNode, {ProcessAccumulator, Uses, Funs} = Acc) ->
    case Option of
        {bucket_type, FilteredBucketType} ->
            FilterFun = fun(BucketType, _BucketName, _Key, _Object, _Binary) ->
                FilteredBucketType =:= BucketType
            end,
            processFilters(Rest, VNode, {ProcessAccumulator, Uses, [FilterFun | Funs]});

        {bucket_name, FilteredBucketName} ->
            FilterFun = fun(_BucketType, BucketName, _Key, _Object, _Binary) ->
                FilteredBucketName =:= BucketName
            end,
            processFilters(Rest, VNode, {ProcessAccumulator, Uses, [FilterFun | Funs]});
        
        {bucket_name_prefix, BucketPrefix} ->
            FilterFun = fun(_BucketType, BucketName, _Key, _Object, _Binary) ->
                isBinaryPrefix(BucketPrefix, BucketName)
            end,
            processFilters(Rest, VNode, {ProcessAccumulator, Uses, [FilterFun | Funs]});

        {key_prefix, KeyPrefix} ->
            FilterFun = fun(_BucketType, _BucketName, Key, _Object, _Binary) ->
                isBinaryPrefix(KeyPrefix, Key)
            end,
            processFilters(Rest, VNode, {ProcessAccumulator, Uses, [FilterFun | Funs]});
            
        {object_size_gte, ThresholdSize} ->
            FilterFun = fun(_BucketType, _BucketName, _Key, _Object, Binary) ->
                erlang:byte_size(Binary) >= ThresholdSize
            end,
            processFilters(Rest, VNode, {ProcessAccumulator, Uses, [FilterFun | Funs]});

        {sibling_count_gte, ThresholdCount} ->
            FilterFun = fun(_BucketType, _BucketName, _Key, Object, _Binary) ->
                length(riak_object:get_values(Object)) >= ThresholdCount
            end,
            processFilters(Rest, VNode, {ProcessAccumulator, true, [FilterFun | Funs]});

        {create_date_lte, ThresholdCount} ->
            FilterFun = fun(_BucketType, _BucketName, _Key, Object, _Binary) ->
                length(riak_object:get_values(Object)) >= ThresholdCount
            end,
            processFilters(Rest, VNode, {ProcessAccumulator, true, [FilterFun | Funs]});

        is_tombstone ->
            FilterFun = fun(_BucketType, _BucketName, _Key, Object, _Binary) ->
                riak_kv_util:is_x_deleted(Object)
            end,
            processFilters(Rest, VNode, {ProcessAccumulator, true, [FilterFun | Funs]});

        is_not_tombstone ->
            FilterFun = fun(_BucketType, _BucketName, _Key, Object, _Binary) ->
                not(riak_kv_util:is_x_deleted(Object))
            end,
            processFilters(Rest, VNode, {ProcessAccumulator, true, [FilterFun | Funs]});

        {custom_filter_fun, FilterFun, NeedsUnserializedObject} when 
            is_function(FilterFun, 5), is_boolean(NeedsUnserializedObject) ->
            
            case NeedsUnserializedObject of
                true ->
                    processFilters(Rest, VNode, {ProcessAccumulator, true, [FilterFun | Funs]});
                false ->
                    processFilters(Rest, VNode, {ProcessAccumulator, Uses, [FilterFun | Funs]});
                _ ->
                    erlang:error(badarg)
            end;
        _ -> 
            processFilters(Rest, VNode, Acc)
    end.        







%% #################################################################################################
%% #             ###################################################################################
%% # Helper funs ###################################################################################
%% #             ###################################################################################
%% #################################################################################################

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


% Used for sorting an object's siblings in modified timestamp order (most recently modified 
% to least). Duplicated from riak_kv/riak_object (since it's not exported from that module)
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
                    throw(lists:flatten(
                        io_lib:format("Unable to reach the following nodes: ~p", [Failures])))
            end;
        error ->
            error(lists:flatten(io_lib:format("unable to get_object_code(~s)", [Module])))
    end.

% Log the vtag, value and deleted status of a given sibling object
log_sibling_contents(Obj, OutputFilename) ->
    DateModified = calendar:now_to_local_time(
        dict:fetch(<<"X-Riak-Last-Modified">>, Obj#r_content.metadata)),
    Deleted = is_deleted(Obj),
    Vtag = get_vtag(Obj),
    Msg = io_lib:format(
        "~p~n", [{{vtag, Vtag}, {date_modified, DateModified}, {is_deleted, Deleted}}]),
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
    NewObj = riak_object:update_metadata(
        riak_object:update_value(OldObj, CorrectSibling#r_content.value), 
        CorrectSibling#r_content.metadata),
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

        lists:foreach(
            fun(Sibling) -> log_sibling_contents(Sibling, OutputFilename) end, SiblingsByDate),

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

dict_merge_fun(_Key, Value1, Value2) ->
    lager:debug("Merging: Encountered ~p with value ~p and ~p with value ~p", 
        [type_of(Value1), Value1, type_of(Value2), Value2]),
    case {type_of(Value1), type_of(Value2)} of
        {integer,integer} -> Value1 + Value2;
        {string, string} -> [Value1, Value2];
        {string, list} -> [Value1 | Value2];
        {list, list} -> [Value1 | Value2];
        {dict, dict} -> dict:merge(fun dict_merge_fun/3, Value1, Value2);
        {TV1,TV2} -> throw(
            lists:flatten(
                io_lib:format(
                    "Failed to merge.  Encountered ~p with value ~p and ~p with value ~p",
                    [TV1, Value1, TV2, Value2])))
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

send_delete_message_to_vnode(PartitionNumber, BKey, RObj) ->
    VNodeName = list_to_atom(lists:flatten(io_lib:format(
        "proxy_riak_kv_vnode_~s",[integer_to_list(PartitionNumber)]))),
    VNodePid = whereis(VNodeName),
    VNodePid ! {final_delete, BKey, delete_hash(RObj)}.

%% Compute a hash of the deleted object
delete_hash(RObj) ->
    erlang:phash2(RObj, 4294967296).

vnode_get(Bucket, Key) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    vnode_get(Ring, Bucket, Key).

vnode_get(Ring, Bucket, Key) ->
    Responses = query_vnodes(Ring, Bucket, Key),
    Values = get_values_from_responses(Responses),
    resolve_replicas(Values).

query_vnodes(Ring, Bucket, Key) ->
    BKey = {Bucket, Key},
    DocIdx = riak_core_util:chash_key(BKey),
    Preflist = riak_core_ring:preflist(DocIdx, Ring),
    riak_kv_vnode:get(Preflist, BKey, 123456789),
    VNodeCount = length(riak_core_ring:all_owners(Ring)),
    wait_for_responses(VNodeCount,[]).

wait_for_responses(MaxCount) ->
    wait_for_responses(MaxCount,[]).

wait_for_responses(MaxCount, Acc) ->
    wait_for_responses(MaxCount, Acc, 30000).

wait_for_responses(MaxCount, Acc, Timeout) ->
    receive
        {'$gen_event',{r, Msg, PartitionNumber, _ReqId}} ->
            NewAcc = [{PartitionNumber, Msg}|Acc],
            case length(NewAcc) >= MaxCount of
                true -> NewAcc;
                false -> wait_for_responses(MaxCount, NewAcc)
            end
    after Timeout ->
        {error, timeout, {collected_values, Acc}}
    end.

get_values_from_responses(Responses) ->
    %%  Responses should be a proplist in the form of:
    %%  [ {PartitionNumber, Response}, ...]
    {_,ResponseList} = lists:unzip(Responses),
    FilteredList = lists:filter(
        fun(Response) -> 
            case Response of 
                {error, notfound} -> false;
                _ ->
                    true
                end
            end, ResponseList),
    ValueList = lists:map(
        fun(Item) ->
            case Item of
                {ok, RObj} ->
                    RObj;
                _ ->
                    Item
            end
        end, FilteredList),
    case length(ValueList) == 0 of
        true -> notfound;
        false -> ValueList
    end.

resolve_replicas(List) ->
    resolve_replicas(List,start).

resolve_replicas([], Acc) ->
    Acc;

resolve_replicas([H|T], start) ->
    resolve_replicas(T, H);

resolve_replicas([H|T], Acc) ->
    NewAcc = riak_object:syntactic_merge(H, Acc),
    resolve_replicas(T, NewAcc).

clean_cluster() ->
    io:format("Cleaning cluster.~n"),
    vnode_list_util:process_cluster_parallel(".",[direct_delete_object, unlogged]),
    io:format("Removing log files from clean job.~n"),
    os:cmd("rm ./*.log"),
    io:format("Done cleaning cluster.~n~n"),
    ok.


add_index_to_bstr(Name,Index) ->
    B = list_to_binary("_"++integer_to_list(Index)),
    <<Name/binary, B/binary>>.

build_test_data() ->
    io:format("~n~nBuilding Test Data.~n"),
    {ok, C} = riak:local_client(),
    build_simple_data(C, <<"good">>, 100),
    build_tombstone_data(C,<<"dead">>, 100),
    build_time_test_data(C, <<"time">>, 100, 5, 30000),
    io:format("Done Building Test Data.~n~n"),
    ok.

build_simple_data(Client, Bucket, NumObjects) ->
    IndexList = lists:seq(1,NumObjects),
    io:format("Creating ~p objects in bucket ~p.~n",[NumObjects, Bucket]),
    lists:map(fun(I) ->
        Client:put(riak_object:new(
            Bucket, add_index_to_bstr(<<"Key">>, I), add_index_to_bstr(<<"Value">>, I)))
    end, IndexList).

build_tombstone_data(Client, Bucket, NumObjects) ->
    case application:get_env(riak_kv,delete_mode) of
        {ok, keep} ->
           IndexList = lists:seq(1,NumObjects),
           io:format("Creating ~p tombstone objects in bucket ~p.~n",[NumObjects, Bucket]),
            lists:map(fun(I) ->
                Client:put(riak_object:new(
                    Bucket, add_index_to_bstr(<<"Key">>, I), add_index_to_bstr(<<"Value">>, I)))
             end, IndexList),
            lists:map(fun(I) ->
                Client:delete(Bucket, add_index_to_bstr(<<"Key">>, I))
            end, IndexList);
        _ ->
            lager:info("Skipping build_tombstone_data, delete_mode is not keep"),
            io:format("Skipping build_tombstone_data, delete_mode is not keep")
    end.

build_time_test_data(Client, Bucket, NumObjects, NumIterations, WaitTime) ->
    build_time_test_data(Client, Bucket, NumObjects, NumIterations, WaitTime, 1).

build_time_test_data(Client, Bucket, NumObjects, NumIterations, WaitTime, Iteration) ->
    StartIndex = (Iteration-1) * NumObjects + 1,
    EndIndex = Iteration * NumObjects,
    Time = erlang:now(),
    io:format("Creating ~p objects in batches of ~p with a ~p ms delay between batches",
        [NumObjects*NumIterations, NumObjects, WaitTime]),
    io:format(" -- check console.log for timings~n"),
    lager:info("Creating ~p:~p, ~p objects in bucket <<\"time\">> at ~p -- ~p/~p~n",
        [StartIndex, EndIndex, NumObjects, Time, Iteration, NumIterations]),
    lists:map(fun(I) ->
        Client:put(
            riak_object:new(
                <<"time">>, 
                add_index_to_bstr(<<"Key">>, I), 
                add_index_to_bstr(<<"Value">>, I)))
    end, lists:seq(StartIndex,EndIndex)),
    case Iteration =< NumIterations of
        true ->
            case timer:apply_after(
                WaitTime,
                vnode_list_util, 
                build_time_test_data, 
                [Client, Bucket, NumObjects, NumIterations, WaitTime, Iteration+1]) of 

                Scheduled = {ok,_} ->
                    lager:debug("Scheduled more data to be built in ~p ms.  Result: ~p",
                        [WaitTime, Scheduled]);
                {error,Reason} -> 
                    lager:error("Unable to schedule next time_test_data iteration. Reason:~p",
                        [Reason])
            end;
        false ->
            io:format("Timed Test Data Created"),
            lager:info("Timed Test Data Created"),
            ok
    end.

type_of(X) when is_integer(X)                      -> integer;
type_of(X) when is_float(X)                        -> float;
type_of(X) when is_list(X)                         -> list;
type_of(X) when is_bitstring(X)                    -> bitstring;
type_of(X) when is_binary(X)                       -> binary;
type_of(X) when is_boolean(X)                      -> boolean;
type_of(X) when is_function(X)                     -> function;
type_of(X) when is_pid(X)                          -> pid;
type_of(X) when is_port(X)                         -> port;
type_of(X) when is_reference(X)                    -> reference;
type_of(X) when is_atom(X)                         -> atom;
type_of(X) when is_function(X)                     -> function;
type_of(X) when is_tuple(X), element(1,X) =:= dict -> dict;
type_of(X) when is_tuple(X)                        -> tuple;
type_of(_X)                                        -> unknown.


dict_pretty_print(Dict) when element(1,Dict) =:= dict ->
    io:format("~s~n",[dict_pretty_print(Dict,0)]);

dict_pretty_print(_) ->
    erlang:error(badarg).

%% @private
dict_pretty_print(Dict, Depth)  ->
    dict:fold(
        fun(Key, Value, AccIn) -> [AccIn | pretty_print_element(Key, Value, Depth)] end,
        [], 
        Dict).

%% @private
pretty_print_element(Key, Value, Depth) when element(1,Value) =:= dict ->
    io_lib:format("~s~p: ~n~s",[string:copies(" ",Depth*4),Key,dict_pretty_print(Value,Depth+1)]);

%% @private
pretty_print_element(Key, Value, Depth) ->
    io_lib:format("~s~p: ~p~n",[string:copies(" ",Depth*4),Key,Value]).


-ifdef(TEST).

-endif.

