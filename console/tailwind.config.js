/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: {
          950: "#0a0e14",
          900: "#0f1419",
          800: "#151b23",
          700: "#1c2430",
          600: "#2a3544",
        },
        signal: {
          green: "#3dd68c",
          amber: "#f5a524",
          red: "#f31260",
          blue: "#66b3ff",
          violet: "#a78bfa",
        },
      },
      fontFamily: {
        mono: ["IBM Plex Mono", "ui-monospace", "monospace"],
        sans: ["IBM Plex Sans", "system-ui", "sans-serif"],
      },
    },
  },
  plugins: [],
};
