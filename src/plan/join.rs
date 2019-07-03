//! Equijoin expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::order::Product;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arrange, Arranged};
use differential_dataflow::operators::JoinCore;
use differential_dataflow::trace::TraceReader;

use crate::binding::{AsBinding, Binding};
use crate::plan::{Dependencies, ImplContext, Implementable};
use crate::{
    AttributeBinding, CollectionRelation, Implemented, Relation, ShutdownHandle, TraceValHandle,
    VariableMap, AsValue, Var, AsAid,
};

/// A plan stage joining two source relations on the specified
/// variables. Throws if any of the join variables isn't bound by both
/// sources.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Join<A, V, P1, P2> {
    phantom: std::marker::PhantomData<(A, V)>,
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the left input.
    pub left_plan: Box<P1>,
    /// Plan for the right input.
    pub right_plan: Box<P2>,
}

impl<A, V, P1, P2> Join<A, V, P1, P2>
where
    A: AsAid,
    V: AsValue,
    P1: Implementable<A, V>,
    P2: Implementable<A, V>,
{
    fn attribute_attribute<'b, T, I, S>(
        nested: &mut Iterative<'b, S, u64>,
        context: &mut I,
        target: Var,
        left: AttributeBinding,
        right: AttributeBinding,
    ) -> (Implemented<'b, S, V>, ShutdownHandle)
    where
        T: Timestamp + Lattice,
        I: ImplContext<A, V, T>,
        S: Scope<Timestamp = T>,
    {
        let mut variables = Vec::with_capacity(3);
        variables.push(target);

        let (left_arranged, shutdown_left) = {
            let (mut index, shutdown_button) = if target == left.variables.0 {
                variables.push(left.variables.1);
                context
                    .forward_propose(&left.source_attribute)
                    .expect("forward propose trace does not exist")
                    .import_core(&nested.parent, &left.source_attribute)
            } else if target == left.variables.1 {
                variables.push(left.variables.0);
                context
                    .reverse_propose(&left.source_attribute)
                    .expect("reverse propose trace does not exist")
                    .import_core(&nested.parent, &left.source_attribute)
            } else {
                panic!("Unbound target variable in Attribute<->Attribute join.");
            };

            let frontier = index.trace.advance_frontier().to_vec();
            let forwarded = index.enter_at(nested, move |_, _, time| {
                let mut forwarded = time.clone();
                forwarded.advance_by(&frontier);
                Product::new(forwarded, 0)
            });

            (forwarded, shutdown_button)
        };

        let (right_arranged, shutdown_right) = {
            let (mut index, shutdown_button) = if target == right.variables.0 {
                variables.push(right.variables.1);
                context
                    .forward_propose(&right.source_attribute)
                    .expect("forward propose trace does not exist")
                    .import_core(&nested.parent, &right.source_attribute)
            } else if target == right.variables.1 {
                variables.push(right.variables.0);
                context
                    .reverse_propose(&right.source_attribute)
                    .expect("reverse propose trace does not exist")
                    .import_core(&nested.parent, &right.source_attribute)
            } else {
                panic!("Unbound target variable in Attribute<->Attribute join.");
            };

            let frontier = index.trace.advance_frontier().to_vec();
            let forwarded = index.enter_at(nested, move |_, _, time| {
                let mut forwarded = time.clone();
                forwarded.advance_by(&frontier);
                Product::new(forwarded, 0)
            });

            (forwarded, shutdown_button)
        };

        let tuples = left_arranged.join_core(&right_arranged, move |key: &V, v1, v2| {
            let mut out = Vec::with_capacity(3);
            out.push(key.clone());
            out.push(v1.clone());
            out.push(v2.clone());

            Some(out)
        });

        let mut shutdown_handle = ShutdownHandle::from_button(shutdown_left);
        shutdown_handle.add_button(shutdown_right);

        let relation = CollectionRelation { variables, tuples };

        (Implemented::Collection(relation), shutdown_handle)
    }

    fn collection_collection<'b, T, S, I>(
        nested: &mut Iterative<'b, S, u64>,
        context: &mut I,
        target_variables: &[Var],
        left: CollectionRelation<'b, S, V>,
        right: CollectionRelation<'b, S, V>,
    ) -> (Implemented<'b, S, V>, ShutdownHandle)
    where
        T: Timestamp + Lattice,
        I: ImplContext<A, V, T>,
        S: Scope<Timestamp = T>,
    {
        let mut shutdown_handle = ShutdownHandle::empty();

        let variables = target_variables
            .iter()
            .cloned()
            .chain(
                left.variables()
                    .drain(..)
                    .filter(|x| !target_variables.contains(x)),
            )
            .chain(
                right
                    .variables()
                    .drain(..)
                    .filter(|x| !target_variables.contains(x)),
            )
            .collect();

        let left_arranged: Arranged<
                Iterative<'b, S, u64>,
            TraceValHandle<Vec<V>, Vec<V>, Product<S::Timestamp, u64>, isize>,
            > = {
            let (arranged, shutdown) = left.tuples_by_variables(nested, context, &target_variables);
            shutdown_handle.merge_with(shutdown);
            arranged.arrange()
        };

        let right_arranged: Arranged<
                Iterative<'b, S, u64>,
            TraceValHandle<Vec<V>, Vec<V>, Product<S::Timestamp, u64>, isize>,
            > = {
            let (arranged, shutdown) = right.tuples_by_variables(nested, context, &target_variables);
            shutdown_handle.merge_with(shutdown);
            arranged.arrange()
        };

        let tuples = left_arranged.join_core(&right_arranged, |key: &Vec<V>, v1, v2| {
            Some(
                key.iter()
                    .cloned()
                    .chain(v1.iter().cloned())
                    .chain(v2.iter().cloned())
                    .collect(),
            )
        });

        let relation = CollectionRelation { variables, tuples };

        (Implemented::Collection(relation), shutdown_handle)
    }

    fn collection_attribute<'b, T, S, I>(
        nested: &mut Iterative<'b, S, u64>,
        context: &mut I,
        target_variables: &[Var],
        left: CollectionRelation<'b, S, V>,
        right: AttributeBinding,
    ) -> (Implemented<'b, S, V>, ShutdownHandle)
    where
        T: Timestamp + Lattice,
        I: ImplContext<A, V, T>,
        S: Scope<Timestamp = T>,
    {
        // @TODO specialized implementation

        let (tuples, shutdown_propose) = match context.forward_propose(&right.source_attribute) {
            None => panic!("attribute {:?} does not exist", &right.source_attribute),
            Some(propose_trace) => {
                let frontier: Vec<T> = propose_trace.advance_frontier().to_vec();
                let (propose, shutdown_propose) =
                    propose_trace.import_core(&nested.parent, &right.source_attribute);

                let tuples = propose
                    .enter_at(nested, move |_, _, time| {
                        let mut forwarded = time.clone();
                        forwarded.advance_by(&frontier);
                        Product::new(forwarded, 0)
                    })
                    .as_collection(|e, v| vec![e.clone(), v.clone()]);

                (tuples, shutdown_propose)
            }
        };

        let right_collected = CollectionRelation {
            variables: vec![right.variables.0, right.variables.1],
            tuples,
        };

        let (implemented, mut shutdown_handle) =
            Join::collection_collection(nested, context, target_variables, left, right_collected);

        shutdown_handle.add_button(shutdown_propose);

        (implemented, shutdown_handle)
    }

    //             Some(var) => {
    //                 assert!(*var == self.variables.1);

    //                 let (index, shutdown_button) = context
    //                     .forward_validate(&self.source_attribute)
    //                     .unwrap()
    //                     .import_core(&scope.parent, &self.source_attribute);

    //                 let frontier = index.trace.advance_frontier().to_vec();
    //                 let forwarded = index.enter_at(scope, move |_, _, time| {
    //                     let mut forwarded = time.clone();
    //                     forwarded.advance_by(&frontier);
    //                     Product::new(forwarded, 0)
    //                 });

    //                 (forwarded, ShutdownHandle::from_button(shutdown_button))
    //             }

    //             Some(var) => {
    //                 assert!(*var == self.variables.0);

    //                 let (index, shutdown_button) = context
    //                     .reverse_validate(&self.source_attribute)
    //                     .unwrap()
    //                     .import_core(&scope.parent, &self.source_attribute);

    //                 let frontier = index.trace.advance_frontier().to_vec();
    //                 let forwarded = index.enter_at(scope, move |_, _, time| {
    //                     let mut forwarded = time.clone();
    //                     forwarded.advance_by(&frontier);
    //                     Product::new(forwarded, 0)
    //                 });

    //                 (forwarded, ShutdownHandle::from_button(shutdown_button))
    //             }
}

impl<A, V, P1, P2> Implementable<A, V> for Join<A, V, P1, P2>
where
    A: AsAid,
    V: AsValue,
    P1: Implementable<A, V>,
    P2: Implementable<A, V>,
{
    fn dependencies(&self) -> Dependencies<A> {
        Dependencies::merge(
            self.left_plan.dependencies(),
            self.right_plan.dependencies(),
        )
    }

    fn into_bindings(&self) -> Vec<Binding<V>> {
        let mut left_bindings = self.left_plan.into_bindings();
        let mut right_bindings = self.right_plan.into_bindings();

        let mut bindings = Vec::with_capacity(left_bindings.len() + right_bindings.len());
        bindings.append(&mut left_bindings);
        bindings.append(&mut right_bindings);

        bindings
    }

    fn datafy(&self) -> Vec<(V, A, V)> {
        let eid = V::gen_eid();

        let mut left_data = self.left_plan.datafy();
        let mut right_data = self.right_plan.datafy();

        let mut left_eids: Vec<(V, A, V)> = left_data
            .iter()
            .map(|(e, _, _)| (eid, "df.join/binding".to_string(), *e))
            .collect();

        let mut right_eids: Vec<(V, A, V)> = right_data
            .iter()
            .map(|(e, _, _)| (eid, "df.join/binding".to_string(), *e))
            .collect();

        let mut data = Vec::with_capacity(
            left_data.len() + right_data.len() + left_eids.len() + right_eids.len(),
        );
        data.append(&mut left_data);
        data.append(&mut right_data);
        data.append(&mut left_eids);
        data.append(&mut right_eids);

        data
    }

    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (Implemented<'b, S, V>, ShutdownHandle)
    where
        T: Timestamp + Lattice,
        I: ImplContext<A, V, T>,
        S: Scope<Timestamp = T>,
    {
        assert!(!self.variables.is_empty());

        let (left, shutdown_left) = self
            .left_plan
            .implement(nested, local_arrangements, context);
        let (right, shutdown_right) =
            self.right_plan
                .implement(nested, local_arrangements, context);

        let (implemented, mut shutdown_handle) = match left {
            Implemented::Attribute(left) => {
                match right {
                    Implemented::Attribute(right) => {
                        if self.variables.len() == 1 {
                            Join::attribute_attribute(nested, context, self.variables[0], left, right)
                        } else if self.variables.len() == 2 {
                            unimplemented!();
                        // intersect_attributes(nested, context, self.variables, left, right)
                        } else {
                            panic!(
                                "Attribute<->Attribute joins can't target more than two variables."
                            );
                        }
                    }
                    Implemented::Collection(right) => {
                        Join::collection_attribute(nested, context, &self.variables, right, left)
                    }
                }
            }
            Implemented::Collection(left) => match right {
                Implemented::Attribute(right) => {
                    Join::collection_attribute(nested, context, &self.variables, left, right)
                }
                Implemented::Collection(right) => {
                    Join::collection_collection(nested, context, &self.variables, left, right)
                }
            },
        };

        shutdown_handle.merge_with(shutdown_left);
        shutdown_handle.merge_with(shutdown_right);

        (implemented, shutdown_handle)
    }
}
