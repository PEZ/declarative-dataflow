//! Types and traits for implementing query plans.

use std::collections::HashSet;
use std::ops::Deref;
use std::sync::atomic::{self, AtomicUsize};

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use crate::binding::{AsBinding, AttributeBinding, Binding};
use crate::Rule;
use crate::{Aid, Eid, Value, Var};
use crate::{
    CollectionRelation, Implemented, Relation, RelationHandle, ShutdownHandle, VariableMap,
};
use crate::{TraceKeyHandle, TraceValHandle};

#[cfg(feature = "set-semantics")]
pub mod aggregate;
#[cfg(not(feature = "set-semantics"))]
pub mod aggregate_neu;
pub mod antijoin;
pub mod filter;
#[cfg(feature = "graphql")]
pub mod graphql;
#[cfg(feature = "graphql")]
pub mod graphql_v2;
pub mod hector;
pub mod join;
pub mod project;
pub mod pull;
pub mod pull_v2;
pub mod transform;
pub mod union;

#[cfg(feature = "set-semantics")]
pub use self::aggregate::{Aggregate, AggregationFn};
#[cfg(not(feature = "set-semantics"))]
pub use self::aggregate_neu::{Aggregate, AggregationFn};
pub use self::antijoin::Antijoin;
pub use self::filter::{Filter, Predicate};
#[cfg(feature = "graphql")]
pub use self::graphql::GraphQl;
pub use self::hector::Hector;
pub use self::join::Join;
pub use self::project::Project;
pub use self::pull::{Pull, PullAll, PullLevel};
pub use self::transform::{Function, Transform};
pub use self::union::Union;

static ID: AtomicUsize = AtomicUsize::new(0);
static SYM: AtomicUsize = AtomicUsize::new(std::usize::MAX);

/// @FIXME
pub fn next_id() -> Eid {
    ID.fetch_add(1, atomic::Ordering::SeqCst) as Eid
}

/// @FIXME
pub fn gensym() -> Var {
    SYM.fetch_sub(1, atomic::Ordering::SeqCst) as Var
}

/// A thing that can provide global state required during the
/// implementation of plans.
pub trait ImplContext<T>
where
    T: Timestamp + Lattice,
{
    /// Returns the definition for the rule of the given name.
    fn rule(&self, name: &str) -> Option<&Rule>;

    /// Returns a mutable reference to a (non-base) relation, if one
    /// is registered under the given name.
    fn global_arrangement(&mut self, name: &str) -> Option<&mut RelationHandle<T>>;

    /// Checks whether an attribute of that name exists.
    fn has_attribute(&self, name: &str) -> bool;

    /// Retrieves the forward count trace for the specified aid.
    fn forward_count(&mut self, name: &str) -> Option<&mut TraceKeyHandle<Value, T, isize>>;

    /// Retrieves the forward propose trace for the specified aid.
    fn forward_propose(
        &mut self,
        name: &str,
    ) -> Option<&mut TraceValHandle<Value, Value, T, isize>>;

    /// Retrieves the forward validate trace for the specified aid.
    fn forward_validate(
        &mut self,
        name: &str,
    ) -> Option<&mut TraceKeyHandle<(Value, Value), T, isize>>;

    /// Retrieves the reverse count trace for the specified aid.
    fn reverse_count(&mut self, name: &str) -> Option<&mut TraceKeyHandle<Value, T, isize>>;

    /// Retrieves the reverse propose trace for the specified aid.
    fn reverse_propose(
        &mut self,
        name: &str,
    ) -> Option<&mut TraceValHandle<Value, Value, T, isize>>;

    /// Retrieves the reverse validate trace for the specified aid.
    fn reverse_validate(
        &mut self,
        name: &str,
    ) -> Option<&mut TraceKeyHandle<(Value, Value), T, isize>>;

    /// Returns the current opinion as to whether this rule is
    /// underconstrained. Underconstrained rules cannot be safely
    /// materialized and re-used on their own (i.e. without more
    /// specific constraints).
    fn is_underconstrained(&self, name: &str) -> bool;
}

/// Description of everything a plan needs prior to synthesis.
pub struct Dependencies {
    /// NameExpr's used by this plan.
    pub names: HashSet<String>,
    /// Attributes queries in Match* expressions.
    pub attributes: HashSet<Aid>,
}

impl Dependencies {
    /// A description representing a dependency on nothing.
    pub fn none() -> Dependencies {
        Dependencies {
            names: HashSet::new(),
            attributes: HashSet::new(),
        }
    }

    /// A description representing a dependency on a single name.
    pub fn name(name: &str) -> Dependencies {
        let mut names = HashSet::new();
        names.insert(name.to_string());

        Dependencies {
            names,
            attributes: HashSet::new(),
        }
    }

    /// A description representing a dependency on a single attribute.
    pub fn attribute(aid: &str) -> Dependencies {
        let mut attributes = HashSet::new();
        attributes.insert(aid.to_string());

        Dependencies {
            names: HashSet::new(),
            attributes,
        }
    }

    /// Merges two dependency descriptions into one, representing
    /// their union.
    pub fn merge(left: Dependencies, right: Dependencies) -> Dependencies {
        Dependencies {
            names: left.names.union(&right.names).cloned().collect(),
            attributes: left.attributes.union(&right.attributes).cloned().collect(),
        }
    }
}

/// A type that can be implemented as a simple relation.
pub trait Implementable {
    /// Returns names of any other implementable things that need to
    /// be available before implementing this one. Attributes are not
    /// mentioned explicitley as dependencies.
    fn dependencies(&self) -> Dependencies;

    /// Transforms an implementable into an equivalent set of bindings
    /// that can be unified by Hector.
    fn into_bindings(&self) -> Vec<Binding> {
        panic!("This plan can't be implemented via Hector.");
    }

    /// @TODO
    fn datafy(&self) -> Vec<(Eid, Aid, Value)> {
        Vec::new()
    }

    /// Implements the type as a simple relation.
    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (Implemented<'b, S>, ShutdownHandle)
    where
        T: Timestamp + Lattice,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>;
}

/// Possible query plan types.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Plan {
    /// Projection
    Project(Project<Plan>),
    /// Aggregation
    Aggregate(Aggregate<Plan>),
    /// Union
    Union(Union<Plan>),
    /// Equijoin
    Join(Join<Plan, Plan>),
    /// WCO
    Hector(Hector),
    /// Antijoin
    Antijoin(Antijoin<Plan, Plan>),
    /// Negation
    Negate(Box<Plan>),
    /// Filters bindings by one of the built-in predicates
    Filter(Filter<Plan>),
    /// Transforms a binding by a function expression
    Transform(Transform<Plan>),
    /// Data pattern of the form [?e a ?v]
    MatchA(Var, Aid, Var),
    /// Data pattern of the form [e a ?v]
    MatchEA(Eid, Aid, Var),
    /// Data pattern of the form [?e a v]
    MatchAV(Var, Aid, Value),
    /// Sources data from another relation.
    NameExpr(Vec<Var>, String),
    /// Pull expression
    Pull(Pull<Plan>),
    /// Single-level pull expression
    PullLevel(PullLevel<Plan>),
    /// Single-level pull expression
    PullAll(PullAll),
    /// GraphQl pull expression
    #[cfg(feature = "graphql")]
    GraphQl(GraphQl),
}

impl Plan {
    /// Returns the variables bound by this plan.
    pub fn variables(&self) -> Vec<Var> {
        match *self {
            Plan::Project(ref projection) => projection.variables.clone(),
            Plan::Aggregate(ref aggregate) => aggregate.variables.clone(),
            Plan::Union(ref union) => union.variables.clone(),
            Plan::Join(ref join) => join.variables.clone(),
            Plan::Hector(ref hector) => hector.variables.clone(),
            Plan::Antijoin(ref antijoin) => antijoin.variables.clone(),
            Plan::Negate(ref plan) => plan.variables(),
            Plan::Filter(ref filter) => filter.variables.clone(),
            Plan::Transform(ref transform) => transform.variables.clone(),
            Plan::MatchA(e, _, v) => vec![e, v],
            Plan::MatchEA(_, _, v) => vec![v],
            Plan::MatchAV(e, _, _) => vec![e],
            Plan::NameExpr(ref variables, ref _name) => variables.clone(),
            Plan::Pull(ref pull) => pull.variables.clone(),
            Plan::PullLevel(ref path) => path.variables.clone(),
            Plan::PullAll(ref path) => path.variables.clone(),
            #[cfg(feature = "graphql")]
            Plan::GraphQl(_) => unimplemented!(),
        }
    }
}

impl Implementable for Plan {
    fn dependencies(&self) -> Dependencies {
        // @TODO provide a general fold for plans
        match *self {
            Plan::Project(ref projection) => projection.dependencies(),
            Plan::Aggregate(ref aggregate) => aggregate.dependencies(),
            Plan::Union(ref union) => union.dependencies(),
            Plan::Join(ref join) => join.dependencies(),
            Plan::Hector(ref hector) => hector.dependencies(),
            Plan::Antijoin(ref antijoin) => antijoin.dependencies(),
            Plan::Negate(ref plan) => plan.dependencies(),
            Plan::Filter(ref filter) => filter.dependencies(),
            Plan::Transform(ref transform) => transform.dependencies(),
            Plan::MatchA(_, ref a, _) => Dependencies::attribute(a),
            Plan::MatchEA(_, ref a, _) => Dependencies::attribute(a),
            Plan::MatchAV(_, ref a, _) => Dependencies::attribute(a),
            Plan::NameExpr(_, ref name) => Dependencies::name(name),
            Plan::Pull(ref pull) => pull.dependencies(),
            Plan::PullLevel(ref path) => path.dependencies(),
            Plan::PullAll(ref path) => path.dependencies(),
            #[cfg(feature = "graphql")]
            Plan::GraphQl(ref q) => q.dependencies(),
        }
    }

    fn into_bindings(&self) -> Vec<Binding> {
        // @TODO provide a general fold for plans
        match *self {
            Plan::Project(ref projection) => projection.into_bindings(),
            Plan::Aggregate(ref aggregate) => aggregate.into_bindings(),
            Plan::Union(ref union) => union.into_bindings(),
            Plan::Join(ref join) => join.into_bindings(),
            Plan::Hector(ref hector) => hector.into_bindings(),
            Plan::Antijoin(ref antijoin) => antijoin.into_bindings(),
            Plan::Negate(ref plan) => plan.into_bindings(),
            Plan::Filter(ref filter) => filter.into_bindings(),
            Plan::Transform(ref transform) => transform.into_bindings(),
            Plan::MatchA(e, ref a, v) => vec![Binding::attribute(e, a, v)],
            Plan::MatchEA(match_e, ref a, v) => {
                let e = gensym();
                vec![
                    Binding::attribute(e, a, v),
                    Binding::constant(e, Value::Eid(match_e)),
                ]
            }
            Plan::MatchAV(e, ref a, ref match_v) => {
                let v = gensym();
                vec![
                    Binding::attribute(e, a, v),
                    Binding::constant(v, match_v.clone()),
                ]
            }
            Plan::NameExpr(_, ref _name) => unimplemented!(), // @TODO hmm...
            Plan::Pull(ref pull) => pull.into_bindings(),
            Plan::PullLevel(ref path) => path.into_bindings(),
            Plan::PullAll(ref path) => path.into_bindings(),
            #[cfg(feature = "graphql")]
            Plan::GraphQl(ref q) => q.into_bindings(),
        }
    }

    fn datafy(&self) -> Vec<(Eid, Aid, Value)> {
        // @TODO provide a general fold for plans
        match *self {
            Plan::Project(ref projection) => projection.datafy(),
            Plan::Aggregate(ref aggregate) => aggregate.datafy(),
            Plan::Union(ref union) => union.datafy(),
            Plan::Join(ref join) => join.datafy(),
            Plan::Hector(ref hector) => hector.datafy(),
            Plan::Antijoin(ref antijoin) => antijoin.datafy(),
            Plan::Negate(ref plan) => plan.datafy(),
            Plan::Filter(ref filter) => filter.datafy(),
            Plan::Transform(ref transform) => transform.datafy(),
            Plan::MatchA(_e, ref a, _v) => vec![(
                next_id(),
                "df.pattern/a".to_string(),
                Value::Aid(a.to_string()),
            )],
            Plan::MatchEA(e, ref a, _) => vec![
                (next_id(), "df.pattern/e".to_string(), Value::Eid(e)),
                (
                    next_id(),
                    "df.pattern/a".to_string(),
                    Value::Aid(a.to_string()),
                ),
            ],
            Plan::MatchAV(_, ref a, ref v) => vec![
                (
                    next_id(),
                    "df.pattern/a".to_string(),
                    Value::Aid(a.to_string()),
                ),
                (next_id(), "df.pattern/v".to_string(), v.clone()),
            ],
            Plan::NameExpr(_, ref _name) => Vec::new(),
            Plan::Pull(ref pull) => pull.datafy(),
            Plan::PullLevel(ref path) => path.datafy(),
            Plan::PullAll(ref path) => path.datafy(),
            #[cfg(feature = "graphql")]
            Plan::GraphQl(ref q) => q.datafy(),
        }
    }

    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (Implemented<'b, S>, ShutdownHandle)
    where
        T: Timestamp + Lattice,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        match *self {
            Plan::Project(ref projection) => {
                projection.implement(nested, local_arrangements, context)
            }
            Plan::Aggregate(ref aggregate) => {
                aggregate.implement(nested, local_arrangements, context)
            }
            Plan::Union(ref union) => union.implement(nested, local_arrangements, context),
            Plan::Join(ref join) => join.implement(nested, local_arrangements, context),
            Plan::Hector(ref hector) => hector.implement(nested, local_arrangements, context),
            Plan::Antijoin(ref antijoin) => antijoin.implement(nested, local_arrangements, context),
            Plan::Negate(ref plan) => {
                let (relation, mut shutdown_handle) =
                    plan.implement(nested, local_arrangements, context);
                let variables = relation.variables();

                let tuples = {
                    let (projected, shutdown) = relation.projected(nested, context, &variables);
                    shutdown_handle.merge_with(shutdown);

                    projected.negate()
                };

                (
                    Implemented::Collection(CollectionRelation { variables, tuples }),
                    shutdown_handle,
                )
            }
            Plan::Filter(ref filter) => filter.implement(nested, local_arrangements, context),
            Plan::Transform(ref transform) => {
                transform.implement(nested, local_arrangements, context)
            }
            Plan::MatchA(e, ref a, v) => {
                let binding = AttributeBinding {
                    variables: (e, v),
                    source_attribute: a.to_string(),
                };

                (Implemented::Attribute(binding), ShutdownHandle::empty())
            }
            Plan::MatchEA(match_e, ref a, sym1) => {
                let (tuples, shutdown_propose) = match context.forward_propose(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(propose_trace) => {
                        let (propose, shutdown_propose) =
                            propose_trace.import_frontier(&nested.parent, a);

                        let tuples = propose
                            .enter(nested)
                            .filter(move |e, _v| *e == Value::Eid(match_e))
                            .as_collection(|_e, v| vec![v.clone()]);

                        (tuples, shutdown_propose)
                    }
                };

                let relation = CollectionRelation {
                    variables: vec![sym1],
                    tuples,
                };

                (
                    Implemented::Collection(relation),
                    ShutdownHandle::from_button(shutdown_propose),
                )
            }
            Plan::MatchAV(sym1, ref a, ref match_v) => {
                let (tuples, shutdown_propose) = match context.forward_propose(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(propose_trace) => {
                        let match_v = match_v.clone();
                        let (propose, shutdown_propose) =
                            propose_trace.import_core(&nested.parent, a);

                        let tuples = propose
                            .enter(nested)
                            .filter(move |_e, v| *v == match_v)
                            .as_collection(|e, _v| vec![e.clone()]);

                        (tuples, shutdown_propose)
                    }
                };

                let relation = CollectionRelation {
                    variables: vec![sym1],
                    tuples,
                };

                (
                    Implemented::Collection(relation),
                    ShutdownHandle::from_button(shutdown_propose),
                )
            }
            Plan::NameExpr(ref syms, ref name) => {
                if context.is_underconstrained(name) {
                    match local_arrangements.get(name) {
                        None => panic!("{:?} not in relation map", name),
                        Some(named) => {
                            let relation = CollectionRelation {
                                variables: syms.clone(),
                                tuples: named.deref().clone(), // @TODO re-use variable directly?
                            };

                            (Implemented::Collection(relation), ShutdownHandle::empty())
                        }
                    }
                } else {
                    // If a rule is not underconstrained, we can
                    // safely re-use it. @TODO it's debatable whether
                    // we should then immediately assume that it is
                    // available as a global arrangement, but we'll do
                    // so for now.

                    match context.global_arrangement(name) {
                        None => panic!("{:?} not in query map", name),
                        Some(named) => {
                            let (arranged, shutdown_button) =
                                named.import_frontier(&nested.parent, name);

                            let relation = CollectionRelation {
                                variables: syms.clone(),
                                tuples: arranged
                                    .enter(nested)
                                    // @TODO this destroys all the arrangement re-use
                                    .as_collection(|tuple, _| tuple.clone()),
                            };

                            (
                                Implemented::Collection(relation),
                                ShutdownHandle::from_button(shutdown_button),
                            )
                        }
                    }
                }
            }
            Plan::Pull(ref pull) => pull.implement(nested, local_arrangements, context),
            Plan::PullLevel(ref path) => path.implement(nested, local_arrangements, context),
            Plan::PullAll(ref path) => path.implement(nested, local_arrangements, context),
            #[cfg(feature = "graphql")]
            Plan::GraphQl(ref query) => query.implement(nested, local_arrangements, context),
        }
    }
}
