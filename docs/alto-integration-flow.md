Property Eye’s Alto integration uses Zoopla’s Alto APIs with OAuth2 client‑credentials, plus a production‑only `AgencyRef` that identifies each real agency account. Sandbox works differently: your sandbox Alto UI tenant is already linked to your integration, so no `AgencyRef` is issued or required there.[1][2][3]

---

## Alto integration model

- **Authentication**:

  - Property Eye uses OAuth2 **client‑credentials** against Zoopla/Alto’s token endpoint to obtain a bearer token that represents the Property Eye integration (supplier), not an individual user.[4][5]
  - Every API request includes `Authorization: Bearer <token>`.

- **Sandbox vs production**:
  - **Sandbox** (`sandbox.altotest.co.uk` + `zoopladev` APIs) is pre‑wired so your integration can access a test agency; you do not receive or send `AgencyRef` here.[2][1]
  - **Production** uses the same auth flow, but access is further scoped per agency using an `AgencyRef` value you get via activation emails once your Marketplace listing is live.[3][6]

---

## AgencyRef and activation emails

- When Property Eye is live in the **Alto Marketplace**, an agency can choose to activate the integration from within Alto.[6]
- Alto then sends an **Activation Request Email** to your Connect/Zoopla login email; this email contains an `AgencyRef` that uniquely identifies that agency’s Alto group for your integration.[3]
- Property Eye must:
  - Capture this `AgencyRef` (out‑of‑band, from the email).
  - Store it against the corresponding agency record in your own database.
  - Include it in production API calls in the header/parameter format defined in the Zoopla docs (e.g. as an agency or supplier identifier).[7][8]

In short: **sandbox = no AgencyRef**, **production = AgencyRef required per onboarded agency**.

---

## Recommended data model and admin UX changes

### Backend: database and domain

- Add a nullable string field `agency_ref` (or `alto_agency_ref`) to your `Agency` table/entity:
  - Used only for Alto production calls.
  - Optional/empty for sandbox or non‑Alto agencies.
- Ensure your Alto client methods accept an `agency_ref: str | None` argument even if sandbox ignores it, so the interface is already production‑ready.
- Persist:
  - `alto_agency_ref`
  - Any other Alto identifiers you may receive later (branch IDs, etc.) in dedicated columns or JSON fields.

### Backend: Alto client behaviour

- Keep the existing OAuth2 client‑credentials flow unchanged:
  - Same token endpoint pattern for sandbox and production, with environment‑specific URLs and credentials.[1][4]
- Update request builder so that in **production**:
  - If `agency_ref` is present for that agency, include it in the required header/param (e.g. `X-AgencyRef`), matching Zoopla’s production spec.[8][7]
  - If `agency_ref` is missing for an agency marked as “Alto‑connected”, raise a clear configuration error.
- In **sandbox**, call the same methods but:
  - Use sandbox URLs and credentials.
  - Do not require or send `agency_ref`; the sandbox tenant mapping covers this.[2]

---

## Admin system requirements

Create a simple internal admin UI so staff can attach `AgencyRef` values received via email to agencies.

### Admin page: “Alto Settings”

- Separate admin‑only page (e.g. `/admin/alto-agencies`) with:
  - List of agencies that have Alto enabled.
  - For each agency:
    - Display name, internal ID, environment (sandbox/production), current `agency_ref` (if any).
    - “Edit Alto settings” action.
- Edit form for a single agency:
  - Text field: `AgencyRef` (exactly as received in the activation email).
  - Checkbox/toggle: “Use Alto in production for this agency”.
  - Read‑only fields showing:
    - Alto status (e.g. “Sandbox only”, “Production connected”, “Missing AgencyRef”).
- Validation:
  - Require `agency_ref` when “Use Alto in production” is turned on.
  - Log all changes (old vs new `agency_ref`, user, timestamp) for audit.

### Operational flow

1. Agency activates Property Eye from Alto Marketplace.
2. Your team receives Alto’s Activation Request Email containing the `AgencyRef`.[3]
3. Admin logs into Property Eye, opens “Alto Settings”, finds the agency, and pastes the `AgencyRef` into the form.
4. From that point, all production Alto API calls for that agency include the stored `agency_ref` and return that agency’s data.

---
