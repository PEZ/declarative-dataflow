//! Union expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Threshold;

use crate::binding::Binding;
use crate::plan::{Dependencies, ImplContext, Implementable};
use crate::{CollectionRelation, Implemented, Relation, ShutdownHandle, Var, VariableMap};

/// A plan stage taking the union over its sources. Frontends are
/// responsible to ensure that the sources are union-compatible
/// (i.e. bind all of the same variables in the same order).
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Union<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plans: Vec<P>,
}

impl<P: Implementable> Implementable for Union<P> {
    fn dependencies(&self) -> Dependencies {
        let mut dependencies = Dependencies::none();

        for plan in self.plans.iter() {
            dependencies = Dependencies::merge(dependencies, plan.dependencies());
        }

        dependencies
    }

    fn into_bindings(&self) -> Vec<Binding> {
        self.plans
            .iter()
            .flat_map(Implementable::into_bindings)
            .collect()
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
        use differential_dataflow::AsCollection;
        use timely::dataflow::operators::Concatenate;

        let mut scope = nested.clone();
        let mut shutdown_handle = ShutdownHandle::empty();

        let streams = self.plans.iter().map(|plan| {
            let relation = {
                let (relation, shutdown) = plan.implement(&mut scope, local_arrangements, context);
                shutdown_handle.merge_with(shutdown);
                relation
            };

            let projected = {
                let (projected, shutdown) =
                    relation.projected(&mut scope, context, &self.variables);
                shutdown_handle.merge_with(shutdown);
                projected
            };

            projected.inner
        });

        let concat = nested.concatenate(streams).as_collection();

        let concatenated = CollectionRelation {
            variables: self.variables.to_vec(),
            tuples: concat.distinct(),
        };

        (Implemented::Collection(concatenated), shutdown_handle)
    }
}
