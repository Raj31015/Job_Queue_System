/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        paper: '#f4eee5',
        ink: '#201d1b',
        clay: '#b46a4d',
        moss: '#56644b',
        plum: '#6f5c67',
        oat: '#ddd0bd',
        line: '#d8cab6',
      },
      fontFamily: {
        sans: ['"Avenir Next"', '"Segoe UI"', 'sans-serif'],
        serif: ['"Iowan Old Style"', '"Palatino Linotype"', 'serif'],
      },
      boxShadow: {
        card: '0 18px 40px rgba(78, 58, 42, 0.08)',
      },
      keyframes: {
        floatSlow: {
          '0%, 100%': { transform: 'translateY(0px)' },
          '50%': { transform: 'translateY(-5px)' },
        },
      },
      animation: {
        floatSlow: 'floatSlow 8s ease-in-out infinite',
      },
      backgroundImage: {
        fibers:
          'radial-gradient(circle at 20% 20%, rgba(255,255,255,0.42), transparent 34%), radial-gradient(circle at 80% 0%, rgba(180,106,77,0.08), transparent 28%), radial-gradient(circle at 50% 100%, rgba(86,100,75,0.08), transparent 30%)',
      },
    },
  },
  plugins: [],
}
