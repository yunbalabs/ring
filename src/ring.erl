-module(ring).

%% ring: ring library's entry point.

-export([create/2, from_bin/1, add_node/2, to_bin/1, locate_key/2, owners/1]).

%% API

create(NumPartitions, SeedNode) ->
    chash:fresh(NumPartitions, SeedNode).

from_bin(CHashBin) ->
    chashbin:to_chash(CHashBin).

add_node(CHash, Node) ->
    Owners = chash:nodes(CHash),
    InitNodes = [N || {_, N} <- Owners],
    FinalNodes =
        case lists:member(Node, InitNodes) of
            true ->
                InitNodes;
            false ->
                [Node | InitNodes]
        end,
    FinalNodes2 = lists:usort(FinalNodes),
    lists:foldl(
        fun(EachNode, CHash0) ->
            claim_until_balanced(CHash0, FinalNodes2, EachNode)
        end, CHash, FinalNodes2).

to_bin(CHash) ->
    chashbin:create(CHash).

locate_key(CHashBin, Key) ->
    Index = chash:key_of(Key),
    Itr = chashbin:iterator(Index, CHashBin),
    {Primaries, _} = chashbin:itr_pop(1, Itr),
    Primaries.

owners(CHash) ->
    chash:nodes(CHash).

%% Internals
claim_until_balanced(CHash, FinalNodes, EachNode) ->
    case wants_claim_v2(CHash, FinalNodes, EachNode) of
        no ->
            CHash;
        {yes, _NumToClaim} ->
            NewCHash =
                case choose_claim_v2(CHash, FinalNodes, EachNode) of
                    [] ->
                        CHash;
                    Indexes ->
                        lists:foldl(
                            fun(Idx, CHash0) ->
                                case chash:lookup(Idx, CHash0) of
                                    EachNode ->
                                        CHash0;
                                    _ ->
                                        chash:update(Idx, EachNode, CHash0)
                                end
                            end, CHash, Indexes)
                end,
            claim_until_balanced(NewCHash, FinalNodes, EachNode)
    end.

%% return the integer is the number of additional partitions wanted by this node.
wants_claim_v2(CHash, AllNodes, Node) ->
    Owners = chash:nodes(CHash),
    Counts = get_counts(AllNodes, Owners),
    NodeCount = erlang:length(AllNodes),
    RingSize = chash:size(CHash),
    Avg = RingSize div NodeCount,
    Count = proplists:get_value(Node, Counts, 0),
    case Count < Avg of
        false ->
            no;
        true ->
            {yes, Avg - Count}
    end.

%% return indexs of all partitions wanted by this node.
choose_claim_v2(CHash, AllNodes, Node) ->
    Params = [{target_n_val, 1}],

    Owners = chash:nodes(CHash),
    Counts = get_counts(AllNodes, Owners),
    RingSize = chash:size(CHash),
    NodeCount = erlang:length(AllNodes),
    Avg = RingSize div NodeCount,
    ActiveDeltas = [{Member, Avg - Count} || {Member, Count} <- Counts],
    Deltas = add_default_deltas(Owners, ActiveDeltas, 0),
    {_, Want} = lists:keyfind(Node, 1, Deltas),
    TargetN = proplists:get_value(target_n_val, Params),
    AllIndices = lists:zip(lists:seq(0, length(Owners)-1),
        [Idx || {Idx, _} <- Owners]),

    EnoughNodes =
        (NodeCount > TargetN)
            or ((NodeCount == TargetN) and (RingSize rem TargetN =:= 0)),
    case EnoughNodes of
        true ->
            %% If we have enough nodes to meet target_n, then we prefer to
            %% claim indices that are currently causing violations, and then
            %% fallback to indices in linear order. The filtering steps below
            %% will ensure no new violations are introduced.
            Violated = lists:flatten(find_violations(Owners, TargetN)),
            Violated2 = [lists:keyfind(Idx, 2, AllIndices) || Idx <- Violated],
            Indices = Violated2 ++ (AllIndices -- Violated2);
        false ->
            %% If we do not have enough nodes to meet target_n, then we prefer
            %% claiming the same indices that would occur during a
            %% re-diagonalization of the ring with target_n nodes, falling
            %% back to linear offsets off these preferred indices when the
            %% number of indices desired is less than the computed set.
            Padding = lists:duplicate(TargetN, undefined),
            Expanded = lists:sublist(AllNodes ++ Padding, TargetN),
            PreferredClaim = diagonal_stripe(Owners, Expanded),
            PreferredNth = [begin
                                {Nth, Idx} = lists:keyfind(Idx, 2, AllIndices),
                                Nth
                            end || {Idx,Owner} <- PreferredClaim,
                Owner =:= Node],
            Offsets = lists:seq(0, RingSize div length(PreferredNth)),
            AllNth = lists:sublist([(X+Y) rem RingSize || Y <- Offsets,
                X <- PreferredNth],
                RingSize),
            Indices = [lists:keyfind(Nth, 1, AllIndices) || Nth <- AllNth]
    end,

    %% Filter out indices that conflict with the node's existing ownership
    Indices2 = prefilter_violations(Owners, Node, AllIndices, Indices,
        TargetN, RingSize),
    %% Claim indices from the remaining candidate set
    Claim = select_indices(Owners, Deltas, Indices2, TargetN, RingSize),
    Claim2 = lists:sublist(Claim, Want),

    RingChanged = ([] /= Claim2),
    RingMeetsTargetN = meets_target_n(Owners, TargetN),
    case {RingChanged, EnoughNodes, RingMeetsTargetN} of
        {false, _, _} ->
            %% Unable to claim, fallback to re-diagonalization
            [];
        {_, true, false} ->
            %% Failed to meet target_n, fallback to re-diagonalization
            [];
        _ ->
            Claim2
    end.

%% @private
%%
%% @doc Counts up the number of partitions owned by each node.
get_counts(Nodes, Ring) ->
    Empty = [{Node, 0} || Node <- Nodes],
    Counts = lists:foldl(fun({_Idx, Node}, Counts) ->
        case lists:member(Node, Nodes) of
            true ->
                dict:update_counter(Node, 1, Counts);
            false ->
                Counts
        end
                         end, dict:from_list(Empty), Ring),
    dict:to_list(Counts).

%% @private
add_default_deltas(IdxOwners, Deltas, Default) ->
    {_, Owners} = lists:unzip(IdxOwners),
    Owners2 = lists:usort(Owners),
    Defaults = [{Member, Default} || Member <- Owners2],
    lists:ukeysort(1, Deltas ++ Defaults).

diagonal_stripe(Owners, Nodes) ->
    %% diagonal stripes guarantee most disperse data
    Partitions = lists:sort([ I || {I, _} <- Owners ]),
    Zipped = lists:zip(Partitions,
        lists:sublist(
            lists:flatten(
                lists:duplicate(
                    1+(length(Partitions) div length(Nodes)),
                    Nodes)),
            1, length(Partitions))),
    Zipped.

%% @private
%%
%% @doc Filter out candidate indices that would violate target_n given
%% a node's current partition ownership.
prefilter_violations(Owners, Node, AllIndices, Indices, TargetN, RingSize) ->
    CurrentIndices = [Idx || {Idx, Owner} <- Owners, Owner =:= Node],
    CurrentNth = [lists:keyfind(Idx, 2, AllIndices) || Idx <- CurrentIndices],
    [{Nth, Idx} || {Nth, Idx} <- Indices,
        lists:all(fun({CNth, _}) ->
            spaced_by_n(CNth, Nth, TargetN, RingSize)
                  end, CurrentNth)].

%% @private
%%
%% @doc Determine if two positions in the ring meet target_n spacing.
spaced_by_n(NthA, NthB, TargetN, RingSize) ->
    case NthA > NthB of
        true ->
            NFwd = NthA - NthB,
            NBack = NthB - NthA + RingSize;
        false ->
            NFwd = NthA - NthB + RingSize,
            NBack = NthB - NthA
    end,
    (NFwd >= TargetN) and (NBack >= TargetN).

%% @private
%%
%% @doc Select indices from a given candidate set, according to two
%% goals.
%%
%% 1. Ensure greedy/local target_n spacing between indices. Note that this
%%    goal intentionally does not reject overall target_n violations.
%%
%% 2. Select indices based on the delta between current ownership and
%%    expected ownership. In other words, if A owns 5 partitions and
%%    the desired ownership is 3, then we try to claim at most 2 partitions
%%    from A.
select_indices(_Owners, _Deltas, [], _TargetN, _RingSize) ->
    [];
select_indices(Owners, Deltas, Indices, TargetN, RingSize) ->
    OwnerDT = dict:from_list(Owners),
    {FirstNth, _} = hd(Indices),
    %% The `First' symbol indicates whether or not this is the first
    %% partition to be claimed by this node.  This assumes that the
    %% node doesn't already own any partitions.  In that case it is
    %% _always_ safe to claim the first partition that another owner
    %% is willing to part with.  It's the subsequent partitions
    %% claimed by this node that must not break the target_n invariant.
    {Claim, _, _, _} =
        lists:foldl(fun({Nth, Idx}, {Out, LastNth, DeltaDT, First}) ->
            Owner = dict:fetch(Idx, OwnerDT),
            Delta = dict:fetch(Owner, DeltaDT),
            MeetsTN = spaced_by_n(LastNth, Nth, TargetN,
                RingSize),
            case (Delta < 0) and (First or MeetsTN) of
                true ->
                    NextDeltaDT =
                        dict:update_counter(Owner, 1, DeltaDT),
                    {[Idx|Out], Nth, NextDeltaDT, false};
                false ->
                    {Out, LastNth, DeltaDT, First}
            end
                    end,
            {[], FirstNth, dict:from_list(Deltas), true},
            Indices),
    lists:reverse(Claim).

meets_target_n(Owners, TargetN) ->
    NewOwners = lists:keysort(1, Owners),
    meets_target_n(NewOwners, TargetN, 0, [], []).
meets_target_n([{Part,Node}|Rest], TargetN, Index, First, Last) ->
    case lists:keytake(Node, 1, Last) of
        {value, {Node, LastIndex, _}, NewLast} ->
            if Index-LastIndex >= TargetN ->
                %% node repeat respects TargetN
                meets_target_n(Rest, TargetN, Index+1, First,
                    [{Node, Index, Part}|NewLast]);
                true ->
                    %% violation of TargetN
                    false
            end;
        false ->
            %% haven't seen this node yet
            meets_target_n(Rest, TargetN, Index+1,
                [{Node, Index}|First], [{Node, Index, Part}|Last])
    end;
meets_target_n([], TargetN, Index, First, Last) ->
    %% start through end guarantees TargetN
    %% compute violations at wrap around, but don't fail
    %% because of them: handle during reclaim
    Violations =
        lists:filter(fun({Node, L, _}) ->
            {Node, F} = proplists:lookup(Node, First),
            (Index-L)+F < TargetN
                     end,
            Last),
    {true, [ Part || {_, _, Part} <- Violations ]}.

find_violations(Owners, TargetN) ->
    Suffix = lists:sublist(Owners, TargetN-1),
    Owners2 = Owners ++ Suffix,
    %% Use a sliding window to determine violations
    {Bad, _} = lists:foldl(fun(P={Idx, Owner}, {Out, Window}) ->
        Window2 = lists:sublist([P|Window], TargetN-1),
        case lists:keyfind(Owner, 2, Window) of
            {PrevIdx, Owner} ->
                {[[PrevIdx, Idx] | Out], Window2};
            false ->
                {Out, Window2}
        end
                           end, {[], []}, Owners2),
    lists:reverse(Bad).
%% End of Module.
