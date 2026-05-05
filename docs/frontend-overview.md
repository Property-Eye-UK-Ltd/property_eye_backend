# Frontend Overview

The Property Eye frontend is a modern web application built with **React**, **TypeScript**, and **Tailwind CSS**. It provides a user interface for agencies to manage their listings, upload reports, and review fraud detections.

---

## Key Pages

- **Dashboard**: High-level overview of agency activity, recent uploads, and detected suspicious matches.
- **Upload**: Interface for uploading CSV/Excel reports and mapping columns to system fields.
- **Reports**: Detailed view of all fraud matches, including Stage 1 (Suspicious) and Stage 2 (Verified) results. Allows for triggering Land Registry verification.
- **Admin**: Agency management, user management, and integration settings.
- **Alto Integration**: Specialized admin page for managing Alto (Zoopla) integration settings and `AgencyRef` values.

---

## Technical Stack

- **Framework**: React 18+
- **Language**: TypeScript
- **Styling**: Tailwind CSS
- **State Management**: React Context API
- **Routing**: React Router
- **HTTP Client**: Axios
- **Build Tool**: Vite

---

## UI Components

The application uses a set of reusable UI components (located in `src/components/ui` if using a library like Shadcn/UI, or `src/components`) for consistency:
- **Data Tables**: For displaying listings and fraud matches.
- **Status Badges**: For visualizing match confidence and verification status.
- **Forms**: For report mapping and settings.
- **Auth Wrappers**: For protecting private routes.
