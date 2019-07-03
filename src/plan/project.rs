//! Projection expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use crate::binding::Binding;
use crate::plan::{Dependencies, ImplContext, Implementable, Var};
use crate::{CollectionRelation, Implemented, Relation, ShutdownHandle, VariableMap};

/// A plan stage projecting its source to only the specified sequence
/// of variables. Throws on unbound variables. Frontends are responsible
/// for ensuring that the source binds all requested variables.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Project<P: Implementable<A, V>> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plan: Box<P>,
}

impl<P: Implementable<A, V>, V> Implementable<A, V> for Project<P> {
    fn dependencies(&self) -> Dependencies<V::Aid> {
        self.plan.dependencies()
    }

    fn into_bindings(&self) -> Vec<Binding> {
        self.plan.into_bindings()
    }

    fn datafy(&self) -> Vec<(V, V::Aid, V)> {
        let eid = V::gen_eid();
        let mut data = self.plan.datafy();

        if data.is_empty() {
            Vec::new()
        } else {
            let child_eid = data[0].0;

            data.push((eid, "df.project/binding".to_string(), child_eid));

            data
        }
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
        let (relation, mut shutdown_handle) =
            self.plan.implement(nested, local_arrangements, context);
        let tuples = {
            let (projected, shutdown) = relation.projected(nested, context, &self.variables);
            shutdown_handle.merge_with(shutdown);

            projected
        };

        let projected = CollectionRelation {
            variables: self.variables.to_vec(),
            tuples,
        };

        (Implemented::Collection(projected), shutdown_handle)
    }
}
