// One grant's controls, for the create form and the access rotation: where its
// credential comes from, and the inputs that source needs. A typed credential
// lives in an uncontrolled input only until the form sends it.
import { useEffect } from "react";
import { Controller, useWatch, type UseFormReturn } from "react-hook-form";
import { Alert, FormSelect, FormSelectOption, Grid, GridItem, TextInput, Title } from "@patternfly/react-core";
import {
  ABSENT_GRANT_MEANING,
  ABSENT_REMOVES,
  ROLE_WORDS,
  SOURCE_WORDS,
  holdsGrant,
  sourcesFor,
  type AccessGrantView,
  type GrantRole,
  type Grants,
  type GrantSource,
} from "../../api/destinations";
import { Field, controlState } from "../connections/parts";

export type GrantsForm = { grants: Grants };

export const EXISTING_ON_ROTATION =
  "Only a Secret this destination already names. To change a credential, enter the new value: it is written to a new Secret bound to this destination, and the old one is left for you to delete.";

function keepWords(view: AccessGrantView | undefined): string {
  if (view && !holdsGrant(view)) return "Keep as it is (none)";
  const what = [view?.mode, view?.secretName, view?.serviceAccountName].filter(Boolean).join(" ");
  return `Keep as it is${what ? ` (${what})` : ""}`;
}

/** Clears every typed credential of the grants (after a submit, and when the form goes away). */
export function clearGrantSecrets(form: UseFormReturn<GrantsForm>, roles: readonly GrantRole[]): void {
  for (const role of roles) {
    form.setValue(`grants.${role}.accessKeyId`, "");
    form.setValue(`grants.${role}.secretAccessKey`, "");
  }
}

export function GrantFields({
  form,
  role,
  kind,
  current,
  secretNames = [],
}: {
  form: UseFormReturn<GrantsForm>;
  role: GrantRole;
  kind: "create" | "rotate";
  current?: AccessGrantView | undefined;
  secretNames?: string[];
}) {
  const { control, register, formState } = form;
  const source = useWatch({ control, name: `grants.${role}.source` }) as GrantSource;
  const errors = formState.errors.grants?.[role];
  const id = (s: string) => `${kind}-${role}-${s}`;

  // A credential typed for "new" does not survive choosing another source.
  useEffect(() => {
    if (source !== "new") clearGrantSecrets(form, [role]);
  }, [source, role, form]);

  return (
    <>
      <Title headingLevel="h4" size="md">
        {ROLE_WORDS[role]}
      </Title>
      <Grid hasGutter md={6}>
        <GridItem>
          <Controller
            name={`grants.${role}.source`}
            control={control}
            render={({ field }) => (
              <Field id={id("source")} label="Credential" isRequired={role === "archiveWrite"} error={errors?.source?.message} help={ABSENT_GRANT_MEANING[role]}>
                <FormSelect {...controlState(id("source"), errors?.source?.message)} value={field.value} onChange={(_e, v) => field.onChange(v)} aria-label={`${ROLE_WORDS[role]} credential`}>
                  {sourcesFor(role, kind).map((s) => (
                    <FormSelectOption key={s} value={s} label={s === "keep" ? keepWords(current) : SOURCE_WORDS[s]} />
                  ))}
                </FormSelect>
              </Field>
            )}
          />
        </GridItem>
        <GridItem>
          {source === "workloadIdentity" && (
            <Controller
              name={`grants.${role}.serviceAccount`}
              control={control}
              render={({ field }) => (
                <Field id={id("sa")} label="ServiceAccount" error={errors?.serviceAccount?.message} help="Blank means logweir-runner.">
                  <TextInput {...controlState(id("sa"), errors?.serviceAccount?.message)} value={field.value} onChange={(_e, v) => field.onChange(v)} />
                </Field>
              )}
            />
          )}
          {source === "existing" && (
            <Controller
              name={`grants.${role}.secret`}
              control={control}
              render={({ field }) => (
                <Field id={id("secret")} label="Secret" error={errors?.secret?.message} help={EXISTING_ON_ROTATION}>
                  <FormSelect {...controlState(id("secret"), errors?.secret?.message)} value={field.value} onChange={(_e, v) => field.onChange(v)}>
                    <FormSelectOption value="" label="Choose a Secret" isPlaceholder />
                    {secretNames.map((n) => (
                      <FormSelectOption key={n} value={n} label={n} />
                    ))}
                  </FormSelect>
                </Field>
              )}
            />
          )}
          {source === "absent" && kind === "rotate" && holdsGrant(current) && ABSENT_REMOVES[role] && (
            <Alert isInline isPlain variant="warning" className="lw-gap-top" title={ABSENT_REMOVES[role]} />
          )}
        </GridItem>
      </Grid>
      {source === "new" && (
        <Grid hasGutter md={6}>
          <GridItem>
            <Field id={id("akid")} label="Access key id" isRequired error={errors?.accessKeyId?.message} help="Entered once.">
              <TextInput {...controlState(id("akid"), errors?.accessKeyId?.message)} autoComplete="off" spellCheck={false} {...register(`grants.${role}.accessKeyId`)} />
            </Field>
          </GridItem>
          <GridItem>
            <Field id={id("sak")} label="Secret access key" isRequired error={errors?.secretAccessKey?.message} help="Entered once. Never shown again.">
              <TextInput {...controlState(id("sak"), errors?.secretAccessKey?.message)} type="password" autoComplete="new-password" {...register(`grants.${role}.secretAccessKey`)} />
            </Field>
          </GridItem>
        </Grid>
      )}
    </>
  );
}
